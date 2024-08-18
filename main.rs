//#[deny(unused_imports)]
//#[warn(unused_imports)]
#[allow(unused_imports)]
use std::collections::HashMap;
use std::env;
use std::string::String;
//use std::sync::Arc;

use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::builders::PutRequestBuilder;
use aws_sdk_dynamodb::types::{AttributeValue, PutRequest, WriteRequest};
use aws_sdk_dynamodb::Client as DynamoClient;
//use aws_sdk_dynamodb::operation::batch_write_item::BatchWriteItemError;
//use aws_smithy_runtime_api::client::result::SdkError;

use uuid::Uuid;

use mysql_async::prelude::*;

//use tokio::task::spawn;
use tokio::time::{sleep, Duration, Instant};

mod types;

const CHILD_UID: u8 = 1;
const _UID_DETACHED: u8 = 3; // soft delete. Child detached from parent.
const OV_BLOCK_UID: u8 = 4; // this entry represents an overflow block. Current batch id contained in Id.
const OV_BATCH_MAX_SIZE: u8 = 5; // overflow batch reached max entries - stop using. Will force creating of new overflow block or a new batch.
const _EDGE_FILTERED: u8 = 6; // set to true when edge fails GQL uid-pred  filter
const _DYNAMO_BATCH_SIZE: usize = 25;
const MAX_TASKS: usize = 1;
const EDGE_MULTIPLY_THRESHOLD: usize = 10;

const LS: u8 = 1;
const LN: u8 = 2;
const LB: u8 = 3;
const LBL: u8 = 4;
const _LDT: u8 = 5;
const Nd: u8 = 6;

type Cuid = Uuid;
type Puid = Uuid;

// Overflow Block (Uuids) item. Include in each propagate item.
// struct OvB {
//      ovb: Vec<AttributeValue>, //uuid.UID // list of node UIDs, overflow block UIDs, oveflow index UIDs
//      xf: Vec<AttributeValue>, // used in uid-predicate 3 : ovefflow UID, 4 : overflow block full
// }

struct DpPropagate {
    //
    nd: Vec<AttributeValue>, // dp parent edge
    ls: Vec<AttributeValue>,
    ln: Vec<AttributeValue>, // merely copying values so keep as Number datatype (no conversion to i64,f64)
    lbl: Vec<AttributeValue>,
    lb: Vec<AttributeValue>,
    ldt: Vec<AttributeValue>,
}

#[::tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Sync + Send + 'static>> {
    // ===============================
    // 1. Source environment variables
    // ===============================
    let mysql_host =
        env::var("MYSQL_HOST").expect("env variable `MYSQL_HOST` should be set in profile");
    let mysql_user =
        env::var("MYSQL_USER").expect("env variable `MYSQL_USER` should be set in profile");
    let mysql_pwd =
        env::var("MYSQL_PWD").expect("env variable `MYSQL_PWD` should be set in profile");
    let mysql_dbname =
        env::var("MYSQL_DBNAME").expect("env variable `MYSQL_DBNAME` should be set in profile");
    let graph = env::var("GRAPH_NAME").expect("env variable `GRAPH_NAME` should be set in profile");

    // ===========================
    // 2. Create a Dynamodb Client
    // ===========================
    let config = aws_config::from_env().region("us-east-1").load().await;
    let dynamo_client = DynamoClient::new(&config);
    // =======================================
    // 3. Fetch Graph Data Types from Dynamodb
    // =======================================
    let (node_types, graph_prefix_wdot) = types::fetch_graph_types(&dynamo_client, graph).await?;
    let graph_sn = graph_prefix_wdot.trim_end_matches('.').to_string();
    println!("Node Types:");
    for t in node_types.0.iter() {
        println!(
            "Node type {} [{}]    reference {}",
            t.get_long(),
            t.get_short(),
            t.is_reference()
        );
        for attr in t {
            println!("attr.name [{}] dt [{}]  c [{}]", attr.name, attr.dt, attr.c);
        }
    }
    let (ty_with_ty_11, ty_with_11) = node_types.get_types_with_11_types();

    println!("ty_with_ty_11 len {}", ty_with_ty_11.len());
    println!("ty_with_11 len {}", ty_with_11.len());
    // ================================
    // 4. Setup a MySQL connection pool
    // ================================
    let pool_opts = mysql_async::PoolOpts::new()
        .with_constraints(mysql_async::PoolConstraints::new(5, 30).unwrap())
        .with_inactive_connection_ttl(Duration::from_secs(60));

    let mysql_pool = mysql_async::Pool::new(
        mysql_async::OptsBuilder::default()
            //.from_url(url)
            .ip_or_hostname(mysql_host)
            .user(Some(mysql_user))
            .pass(Some(mysql_pwd))
            .db_name(Some(mysql_dbname))
            .pool_opts(pool_opts),
    );
    let pool = mysql_pool.clone();
    println!("establishing mysql connection...");
    let mut conn = pool.get_conn().await?;
    println!("mysql connection succeeded");

    // ============================
    // 5. MySQL query: parent nodes
    // ============================
    let mut parent_node: Vec<Uuid> = vec![];

    let parent_edge = "SELECT Uid FROM Edge_test order by cnt desc"
        .with(())
        .map(&mut conn, |puid| parent_node.push(puid))
        .await?;
    // =======================================
    // 6. MySQL query: graph parent node edges
    // =======================================
    let mut parent_edges: HashMap<Puid, HashMap<types::SortK, Vec<Cuid>>> = HashMap::new();
    let child_edge = "Select puid,sortk,cuid from test_childedge order by puid,sortk"
        .with(())
        .map(&mut conn, |(puid, sortk, cuid): (Uuid, String, Uuid)| {
            // this version requires no allocation (cloning) of sortk
            match parent_edges.get_mut(&puid) {
                None => {
                    let mut e = HashMap::new();
                    e.insert(types::SortK::new(&sortk), vec![cuid]);
                    parent_edges.insert(puid, e);
                }
                Some(e) => match e.get_mut(&types::SortK::new(&sortk)) {
                    None => {
                        let e = match parent_edges.get_mut(&puid) {
                            None => {
                                panic!("logic error in parent_edges get_mut()");
                            }
                            Some(e) => e,
                        };
                        e.insert(types::SortK::new(&sortk), vec![cuid]);
                    }
                    Some(c) => {
                        c.push(cuid);
                    }
                },
            }
        })
        .await?;

    // ===========================================
    // 7. Setup asynchronous tasks infrastructure
    // ===========================================
    let mut tasks: usize = 0;
    let (prod_ch, mut task_rx) = tokio::sync::mpsc::channel::<bool>(MAX_TASKS);
    // ====================================
    // 8. Setup retry failed writes channel
    // ====================================
    let (retry_send_ch, mut retry_rx) =
        tokio::sync::mpsc::channel::<Vec<aws_sdk_dynamodb::types::WriteRequest>>(MAX_TASKS);
    let mut i = 0;
    let p_sk_prefix = graph_sn.clone() + "|A#G#:";
    // ==============================================================
    // 9. spawn task to attach node edges and propagate scalar values
    // ==============================================================
    println!("parent_node count {}", parent_node.len());
    for puid in parent_node {
        // TODO: fetch from test_childedge to reduce memory consumption

        let mut sk_edges = match parent_edges.remove(&puid) {
            None => {
                panic!("logic error. No entry found in parent_edges");
            }
            Some(e) => e,
        };
        // =====================================================
        // 9.1 clone enclosed vars before moving into task block
        // =====================================================
        let task_ch = prod_ch.clone();
        let dyn_client = dynamo_client.clone();
        let retry_ch = retry_send_ch.clone();
        let graph_sn = graph_prefix_wdot.trim_end_matches('.').to_string();
        let node_types_ = node_types.clone(); // Arc instance - single cache in heap storage
                                              // =====================================================================
                                              // p_node_ty : find type of puid . use sk "m|T#"  <graph>|<T># //TODO : type short name should be in mysql table - saves fetching here.
                                              // =====================================================================
        println!("fetch_node_type...");
        let p_node_ty = fetch_node_type(&dyn_client, &puid, &graph_sn, &node_types_).await;
        println!("parent node_type {}", p_node_ty.long_nm());
        let Some(p_11_edges) = ty_with_ty_11.get(p_node_ty) else {
            continue;
        }; //Person contains Performance (is_11)
        println!("p_11_edges {}", p_11_edges.len());

        //for (p_sk ,uids) in sk_edges {

        //for (_,p_11_edges) in ty_with_ty_11.get(p_node_ty) { // Person->Performance, Person->?, ...

        // for Person type (contained in ty_with_ty_11)
        // SortK          Executable
        //--------        ---------------
        //A#G#:A        - attach(node) op - Performance edge (source ty_with_ty_11 - Person.attrs[Pf])
        //A#G#:A:?      - attach(sp) op  - propagate Performance edge scalars belonging to performance node (sp - single propagation)

        //A#G#:A#G:A    - dp op - Performance -> Actor edge (source ty_with_11[Performance].attrs)
        //A#G#:A#G#:A#:N  - dp op - scalar double propagation from Performance - Actor edge child node (gen-sortk-dp : append :N)

        //A#G#:A#G:C    - dp op - Performance -> Character edge (source ty_with_11[Performance].attrs)
        //A#G#:A#G:C:N  - dp op - scalar double propagation from Performance - Character edge child node  (source get-scalar[Character])

        //A#G#:A#G:F    - dp op - Performance -> Film Edge (source ty_with_11[Performance].attrs)
        //A#G#:A#G#:F#:N, - dp op - Performance Film - Film scalar (propagated from Performance Film propagated scalar A#G#:F:N) (source get-get_scalars[Film])
        //A#G#:A#G#:F#:Nf - dp op - Performance Film - Film scalar (propagated from Performance Film propagated scalar A#G#:F:Nf)
        //A#G#:A#G#:F#:Rv - dp op - Performance Film - Film scalar
        //A#G#:A#G#:F#:R  - dp op - Performance Film - Film scalar

        // Child with 11 attr(s) e.g. Performance node  - source : ty_with_11.attrs (iterate to attr)
        // A#G#:A - edge 1:1 Actor   -- attrs.ty
        //A#G#:A#:N              -  source : get_scalars for attr.ty
        // A#G#:C - edge 1:1 Character
        //A#G#:C#:N              -  source : get_scalars for attr.ty
        // A#G#:F - edge 1:1 Film
        // A#G#:F#:N             -  source : get_scalars for attr.ty (convert ty to NodeTypes.get(ty) using )
        // A#G#:F#:Nf
        // A#G#:F#:R
        // A#G#:F#:Rv

        // container for all double propagated data for a p_node
        let mut items: HashMap<String, DpPropagate> = HashMap::new();

        for &p_11_edge in p_11_edges {
            // Performance, . .

            let p_11_edge_sk = p_sk_prefix.clone() + p_11_edge.c.as_str(); // "m|A#G#:A";

            println!("p_11_edge_sk [{}]", p_11_edge_sk);

            let Some(c_uids) = sk_edges.remove(&types::SortK::new(&p_11_edge_sk)) else {
                for (k, _) in sk_edges {
                    println!("sk_edegs key {:?}", k)
                }
                panic!("{} not found in sk_edges", &p_11_edge_sk)
            };

            // child type
            let c_type = node_types_.get(&p_11_edge.ty); // Performance
            let Some(c_11_edges) = ty_with_11.get(c_type) else {
                panic!("{:?} not found in ty_with_11", c_type)
            }; // Performance 11 edges: Actor, Character, Film
            let c_edges = c_type.edge_cnt(); // total edge types on parent
                                             // check how many 11 edges for current p_11_edge - basis of sortk for child query
            let c_sk_query = if c_11_edges.len() == 1 {
                vec![p_sk_prefix.clone() + c_11_edges[0].c.as_str()] // child node sortk
            } else {
                match c_edges > c_11_edges.len() * EDGE_MULTIPLY_THRESHOLD {
                    true => {
                        let mut v_sk = vec![];
                        for &e in c_11_edges {
                            v_sk.push(p_sk_prefix.clone() + e.c.as_str())
                        }
                        v_sk
                    }
                    false => vec![p_sk_prefix.clone()], // global sk query "m|A#G#""
                }
            };

            println!("c_sk_query [{:?}]", c_sk_query);

            // propagate Nd data and scalar data of each edge
            for c_uid in c_uids {
                // allocate node cache
                let mut nc_attr_map: types::NodeMap = types::NodeMap(HashMap::new());

                // load cache with results of c_sk_query sortks .
                for c_sk in &c_sk_query {
                    println!("c_sk [{}]", &c_sk);

                    // perform query on cuid, query_sk
                    let result = dyn_client
                        .query()
                        .table_name("RustGraph.dev.2")
                        .key_condition_expression("#p = :uid and begins_with(#s,:sk_v)")
                        .expression_attribute_names("#p", types::PK)
                        .expression_attribute_names("#s", types::SK)
                        .expression_attribute_values(
                            ":uid",
                            AttributeValue::B(Blob::new(c_uid.clone())),
                        )
                        .expression_attribute_values(":sk_v", AttributeValue::S(c_sk.clone()))
                        .send()
                        .await;

                    if let Err(err) = result {
                        panic!("error in query() {}", err);
                    }

                    // ============================================================
                    // 9.2.3.1.2 populate node cach (nc) from query result
                    // ============================================================
                    let mut q: Vec<types::DataItem> = vec![];

                    if let Some(items) = result.unwrap().items {
                        q = items.into_iter().map(|v| v.into()).collect();
                    }

                    for c in q {
                        nc_attr_map.0.insert(c.sk.attribute_sn().to_owned(), c);
                    }
                }
                /*
                double propagate Nd from cache
                */
                let Some(v) = ty_with_11.get(node_types_.get(&p_11_edge.ty[..])) else {
                    panic!("Expected Some got None for {}", p_11_edge.ty.as_str())
                };
                for &pg_11_edge in v {
                    // 1:1 attrs for Performance (Actor,Character,Film)

                    let pg_sk_base = p_11_edge_sk.clone() + "#G#:" + pg_11_edge.c.as_str();
                    println!(
                        "pg_11_edge.c.as_str() [{}]    pg_sk_base [{}]",
                        pg_11_edge.c.as_str(),
                        pg_sk_base
                    );

                    let Some(di) = nc_attr_map.0.remove(pg_11_edge.c.as_str()) else {
                        for (k, _) in nc_attr_map.0 {
                            println!("nc_attr_map key {}", k)
                        }
                        panic!("not found in nc_attr_map [{}]", pg_sk_base);
                    };

                    match pg_11_edge.dt.as_str() {
                        // edge predicate (uuidpred)
                        "Nd" => match di.nd {
                            Some(v) => {
                                for b in v {
                                    items
                                        .entry(pg_sk_base.clone())
                                        .and_modify(|v| {
                                            v.nd.push(AttributeValue::B(Blob::new(b.as_bytes())))
                                        })
                                        .or_insert(DpPropagate {
                                            nd: vec![AttributeValue::B(Blob::new(b.as_bytes()))],
                                            ls: vec![],
                                            ln: vec![],
                                            lbl: vec![],
                                            lb: vec![],
                                            ldt: vec![],
                                        });
                                }
                            }
                            _ => {
                                panic!(
                                    "Data Error: Attribute {} in type {} has no value",
                                    pg_11_edge.c,
                                    c_type.long_nm()
                                )
                            }
                        },
                        _ => {
                            panic!("Edge Query: expected Nd type...")
                        }
                    }
                }
                let Some(c_attrs) = ty_with_11.get(node_types_.get(&p_11_edge.ty[..])) else {
                    panic!("type {} not found in ty_with_11", &p_11_edge.ty[..])
                };
                /*
                double propagate Scalar
                */
                for &pg_11_edge in c_attrs {
                    // 1:1 attrs for Performance (Actor,Character,Film)

                    let pg_sk_base = p_11_edge_sk.clone() + "#G#:" + &pg_11_edge.c[..] + "#:";

                    for (_, s) in &node_types_.get(&p_11_edge.ty[..]).get_scalars() {
                        for &scalar in s {
                            let pg_sk = pg_sk_base.clone() + scalar;

                            println!("pg_sk: {}", pg_sk);

                            let Some(di) = nc_attr_map.0.remove(&pg_sk[..]) else {
                                panic!("not found in nc_attr_map [{}]", pg_sk);
                            };

                            match pg_11_edge.dt.as_str() {
                                // edge predicate (uuidpred)
                                "I" | "F" => match di.ln {
                                    Some(mut vv) => match vv.remove(0) {
                                        Some(s) => match items.get_mut(&pg_sk) {
                                            None => {
                                                items.insert(
                                                    pg_sk.clone(),
                                                    DpPropagate {
                                                        nd: vec![],
                                                        ls: vec![],
                                                        ln: vec![AttributeValue::N(s)],
                                                        lbl: vec![],
                                                        lb: vec![],
                                                        ldt: vec![],
                                                    },
                                                );
                                            }
                                            Some(v) => v.ln.push(AttributeValue::N(s)),
                                        },
                                        _ => panic!(
                                            "expected Some got None in cache for I or F type"
                                        ),
                                    },
                                    _ => {
                                        panic!(
                                            "Data Error: Attribute {} in type {} has no value",
                                            pg_11_edge.c,
                                            c_type.long_nm()
                                        )
                                    }
                                },
                                "S" => match di.ls {
                                    Some(mut vv) => match vv.remove(0) {
                                        Some(s) => match items.get_mut(&pg_sk) {
                                            None => {
                                                items.insert(
                                                    pg_sk.clone(),
                                                    DpPropagate {
                                                        nd: vec![],
                                                        ls: vec![AttributeValue::N(s)],
                                                        ln: vec![],
                                                        lbl: vec![],
                                                        lb: vec![],
                                                        ldt: vec![],
                                                    },
                                                );
                                            }
                                            Some(v) => v.ls.push(AttributeValue::N(s)),
                                        },
                                        _ => panic!("expected Some got None in cache for Stype"),
                                    },
                                    _ => {
                                        panic!(
                                            "Data Error: Attribute {} in type {} has no value",
                                            pg_11_edge.c,
                                            c_type.long_nm()
                                        )
                                    }
                                },
                                "Bl" => match di.lbl {
                                    Some(vv) => match vv[0] {
                                        Some(bl) => match items.get_mut(&pg_sk) {
                                            None => {
                                                items.insert(
                                                    pg_sk.clone(),
                                                    DpPropagate {
                                                        nd: vec![],
                                                        ls: vec![],
                                                        ln: vec![],
                                                        lbl: vec![AttributeValue::Bool(bl)],
                                                        lb: vec![],
                                                        ldt: vec![],
                                                    },
                                                );
                                            }
                                            Some(v) => v.lbl.push(AttributeValue::Bool(bl)),
                                        },
                                        _ => panic!("expected Some got None in cache for Bl type"),
                                    },
                                    _ => {
                                        panic!(
                                            "Data Error: Attribute {} in type {} has no value",
                                            pg_11_edge.c,
                                            c_type.long_nm()
                                        )
                                    }
                                },
                                "B" => {
                                    match di.lb {
                                        Some(vv) => {
                                            for s in vv {
                                                match s {
                                                    Some(s) => {
                                                        items.entry(pg_sk.clone())
                                                             .and_modify( |n| n.lb.push(AttributeValue::B(Blob::new(s.clone()))))
                                                             .or_insert(
                                                                DpPropagate {
                                                                    nd: vec![],
                                                                    ls: vec![],
                                                                    ln: vec![],
                                                                    lbl: vec![],
                                                                    lb: vec![AttributeValue::B(Blob::new(s.clone()))],
                                                                    ldt: vec![],
                                                                }
                                                            );
                                                    },
                                                    _ => panic!("expected Some got None in cache for B type"),                                                 
                                                }
                                            }
                                        }
                                        _ => {
                                            panic!(
                                                "Data Error: Attribute {} in type {} has no value",
                                                pg_11_edge.c,
                                                c_type.long_nm()
                                            )
                                        }
                                    }
                                }
                                _ => {
                                    panic!(
                                        "Unhandled match: got {} in match of dt",
                                        pg_11_edge.dt.as_str()
                                    )
                                }
                            }
                        }
                    }
                }
            }
            // concurrent task counter
            tasks += 1;
        }
    }
    Ok(())
}

// returns node type as String, moving ownership from AttributeValue - preventing further allocation.
async fn fetch_node_type<'a, T: Into<String>>(
    dyn_client: &DynamoClient,
    uid: &Uuid,
    graph_sn: T,
    node_types: &'a types::NodeTypes,
) -> &'a types::NodeType {
    let mut sk_for_type: String = graph_sn.into();
    sk_for_type.push_str("|T#");

    let result = dyn_client
        .get_item()
        .table_name("RustGraph.dev.2")
        .key(types::PK, AttributeValue::B(Blob::new(uid.clone())))
        .key(types::SK, AttributeValue::S(sk_for_type))
        .projection_expression("Ty")
        .send()
        .await;

    if let Err(err) = result {
        panic!(
            "get node type: no item found: expected a type value for node. Error: {}",
            err
        )
    }
    let nd: types::NodeType = match result.unwrap().item {
        None => panic!("No type item found in fetch_node_type() for [{}]", uid),
        Some(v) => v.into(),
    };
    node_types.get(&nd.short_nm())
}
