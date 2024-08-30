//#[deny(unused_imports)]
//#[warn(unused_imports)]
mod service;
mod types;
#[allow(unused_imports)]
use std::collections::HashMap;
use std::env;
use std::process::Child;
use std::string::String;
use std::sync::Arc;
//use std::sync::Arc;

use aws_sdk_dynamodb::operation::query::builders::QueryFluentBuilder;
use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::builders::PutRequestBuilder;
use aws_sdk_dynamodb::types::{AttributeValue, PutRequest, WriteRequest};
use aws_sdk_dynamodb::Client as DynamoClient;
//use aws_sdk_dynamodb::operation::batch_write_item::BatchWriteItemError;
//use aws_smithy_runtime_api::client::result::SdkError;

use uuid::Uuid;

use mysql_async::prelude::*;

//use tokio::task::spawn;
use tokio::sync::broadcast;
use tokio::time::{sleep, Duration, Instant};

// ==============================================================================
// Overflow block properties - consider making part of a graph type specification
// ==============================================================================

// EMBEDDED_CHILD_NODES - number of cUIDs (and the assoicated propagated scalar data) stored in the paraent uid-pred attribute e.g. A#G#:S.
// All uid-preds can be identified by the following sortk: <partitionIdentifier>#G#:<uid-pred-short-name>
// for a parent with limited amount of scalar data the number of embedded child uids can be relatively large. For a parent
// node with substantial scalar data this parameter should be corresponding small (< 5) to minimise the space consumed
// within the parent block. The more space consumed by the embedded child node data the more RCUs required to read the parent Node data,
// which will be an overhead in circumstances where child data is not required.
const EMBEDDED_CHILD_NODES: usize = 4; //10; // prod value: 20

// MAX_OV_BLOCKS - max number of overflow blocks. Set to the desired number of concurrent reads on overflow blocks ie. the degree of parallelism required. Prod may have upto 100.
// As each block resides in its own UUID (PKey) there shoud be little contention when reading them all in parallel. When max is reached the overflow
// blocks are then reused with new overflow items (Identified by an ID at the end of the sortK e.g. A#G#:S#:N#3, here the id is 3)  being added to each existing block
// There is no limit on the number of overflow items, hence no limit on the number of child nodes attached to a parent node.
const MAX_OV_BLOCKS: usize = 5; // prod value : 100

// OV_MAX_BATCH_SIZE - number of items to an overflow batch. Always fixed at this value.
// The limit is checked using the database SIZE function during insert of the child data into the overflow block.
// An overflow block has an unlimited number of batches.
const OV_MAX_BATCH_SIZE: usize = 4; //15; // Prod 100 to 500.

// OV_BATCH_THRESHOLD, initial number of batches in an overflow block before creating new Overflow block.
// Once all overflow blocks have been created (MAX_OV_BLOCKS), blocks are randomly chosen and each block
// can have an unlimited number of batches.
const OV_BATCH_THRESHOLD: usize = 4; //100

const CHILD_UID: u8 = 1;
const _UID_DETACHED: u8 = 3; // soft delete. Child detached from parent.
const OV_BLOCK_UID: u8 = 4; // this entry represents an overflow block. Current batch id contained in Id.
const OV_BATCH_MAX_SIZE: u8 = 5; // overflow batch reached max entries - stop using. Will force creating of new overflow block or a new batch.
const _EDGE_FILTERED: u8 = 6; // set to true when edge fails GQL uid-pred  filter
const DYNAMO_BATCH_SIZE: usize = 25;
const MAX_TASKS: usize = 1; // concurrent propagation into Parent node : Prod 20
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

type SortK = String;

#[derive(Debug)]
struct DpPropagate {
    entry: Option<u8>,
    psk: String,
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
    let table_name = "RustGraph.dev.3"; // "RustGraph.dev.3";
    // ===========================
    // 2. Create a Dynamodb Client
    // ===========================
    let config = aws_config::from_env().region("us-east-1").load().await;
    let dynamo_client = DynamoClient::new(&config);
    // =======================================
    // 3. Fetch Data Types from Dynamodb
    // =======================================
    let (m_node_types, graph_prefix_wdot) = types::fetch_graph_types(&dynamo_client, graph).await?;
    let graph_sn = graph_prefix_wdot.trim_end_matches('.').to_string();
    println!("Node Types:");
    for t in m_node_types.0.iter() {
        println!(
            "Node type {} [{}]    reference {}",
            t.get_long(),
            t.get_short(),
            t.is_reference()
        );
        for attr in t.into_iter() {
            println!("attr.name [{}] dt [{}]  c [{}]", attr.name, attr.dt, attr.c);
        }
    }
    // create broadcast channel to shutdown services
    let (shutdown_broadcast_ch, mut shutdown_recv_ch) = broadcast::channel(1); // broadcast::channel::<u8>(1);
                                                                               // start Retry service (handles failed putitems)
    println!("start Retry service...");
    let (retry_ch, mut retry_rx) = tokio::sync::mpsc::channel(MAX_TASKS * 2);
    let mut retry_shutdown_ch = shutdown_broadcast_ch.subscribe();
    let retry_service = service::retry::start_service(
        dynamo_client.clone(),
        retry_rx,
        retry_ch.clone(),
        retry_shutdown_ch,
        table_name,
    );
    // ===========================================
    // 7. Setup asynchronous tasks infrastructure
    // ===========================================
    let mut node_tasks: usize = 0;
    let (prod_ch, mut node_task_rx) = tokio::sync::mpsc::channel::<bool>(MAX_TASKS);
    // ====================================
    // 8. Setup retry failed writes channel
    // ====================================
    let (retry_send_ch, mut retry_rx) =
        tokio::sync::mpsc::channel::<Vec<aws_sdk_dynamodb::types::WriteRequest>>(MAX_TASKS);
    let m_retry_send_ch = retry_send_ch.clone();
    // ==================================================================================
    // 9. find data types containing nested types with a 1:1 relationship with child type
    // ==================================================================================
    //                  Node:Person[Edges containing types that contain 1:1 : Performance ]
    //                                                              Nodes containing 1:1 edges:Performance[Character, Actor, Genre]
    let (ty_with_ty_11, ty_with_11) = m_node_types.get_types_with_11_types();
    // ==================================================================================
    // 10. for each type containing types that contain 1:1 edges
    // ==================================================================================
    for (p_ty, p_edges_11) in ty_with_ty_11 {
        let mut first = true;
        let graph_sn = graph_sn.clone();
        // ==================================
        // UNDO THE FOLLOWING after testing...
        // ===================================
        let g_p_ty = graph_sn.clone() + "|" + p_ty.short_nm();
        println!("pty = [{}]", g_p_ty);

        // Paginated Query design pattern in Rust
        //
        // 1. On first execution of query pass None to set_exclusive_start_key. This tells Dynamo to produce first page of results.
        // 2. subsequent executions of query return Some value in last_evaluated_key.
        // 3. feed the result of last_evaluated_key back into set_exclusive_start_key
        // 3. when last_evaluated_key returns with None the query has produced its last batch.
        //
        //let g_p_ty = "m|Fm".to_owned();
        let mut more_nodes = true;
        let mut last_key: Option<HashMap<String, AttributeValue>> = None;

        while more_nodes {
            let result = dynamo_client
                .clone()
                .query()
                .table_name(table_name)
                .index_name("TyIx")
                .key_condition_expression("#ix = :ty")
                .expression_attribute_values(":ty", AttributeValue::S(g_p_ty.clone()))
                .expression_attribute_names("#ix", types::TYIX)
                .set_exclusive_start_key(last_key)
                .projection_expression("PK")
                .limit(5)
                .send()
                .await;

            let nodes = match result {
                // issue: cannot have async methods ????
                Err(err) => panic!("Fetch nodes with type query failed:  Error: {}", err),
                Ok(out) => {
                    println!("items count: {}", out.count);
                    last_key = out.last_evaluated_key;
                    if last_key.is_none() {
                        more_nodes = false;
                    }

                    let nd: Vec<Uuid> = match out.items {
                        None => vec![],
                        Some(v) => v
                            .into_iter()
                            .flatten()
                            .filter_map(|(s, v)| if s == types::PK { Some(v) } else { None })
                            .map(|av| types::as_uuid(av).unwrap())
                            .collect(),
                    };
                    nd
                }
            };

            for puid in nodes {
                println!("puid = [{}]", puid.to_string());
                // Peter Sellers [Fi9DhIPlTDKNPGqyuH/xYQ==](no data propagated into Performance nodes - check - run sp first)
                if puid.to_string() != "162f4384-83e5-4c32-8d3c-6ab2b87ff161" {
                    continue;
                }
                println!("    found");
                // ===========================================================
                // 9.1 clone enclosed vars before moving into async task block
                // ===========================================================
                let task_ch = prod_ch.clone();
                let m_dyn_client = dynamo_client.clone();
                let graph_sn = graph_sn.clone();
                let retry_ch_ = retry_send_ch.clone();
                //let node_types = Arc::clone(&m_node_types);
                let node_types = m_node_types.clone();
                let ty_with_11_ = ty_with_11.clone(); // Arc clone not a HashMap clone
                let p_edges_11 = p_edges_11.clone();
                let m_p_edges_11 = p_edges_11.clone();
                let p_ty_ = p_ty.clone();
                node_tasks += 1; // concurrent task counter
                // ================================================================================
                // 9. spawn task to process each puid which will propagate edge data fron child node
                // =================================================================================       
                tokio::spawn(async move {
                    let sk_base = graph_sn.clone() + "|A#G#:";


                    // SortK          Executable
                    //--------        ---------------
                    //A#G#:A        - attach(node) op - Performance edge (source  - Person.attrs[Pf])
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
                    // ==================================================================================
                    // 10. for each parent-edge containing types that contain 1:1 edges
                    // ==================================================================================

                    for p_edge_11 in p_edges_11 {
                        println!("p_edge_11.c {}", p_edge_11.c);

                        // parent-edge SK (Performance)
                        let p_edge_11_sk = sk_base.clone() + p_edge_11.c.as_str(); // m|A#G#: + A;
                        let p_edge_11_sk_ = p_edge_11_sk.clone();

                        println!("p_edge_11_sk [{}]", p_edge_11_sk);
                        // ===================================================================================
                        // 10. fetch nodes associated with current parent-edge containing types with 1:1 edges
                        // ===================================================================================

                        // setup async process for puid-nd & each ovb - process reads a batch (ovb batch size), processes and continues until all batches done.
                        // this design employees parallelism and takes advantage of distributed ovb design to reduce resource contention and reduces
                        // memory consumption as only batches are consumed at a time rather than all uuids being loaded into memory then processed.

                        let (cuids, ovbs, bids) =
                            fetch_child_nodes(&m_dyn_client, &puid, &p_edge_11_sk, table_name)
                                .await;
                        let mut ovb_pk: HashMap<String, Vec<Uuid>> = HashMap::new();
                        ovb_pk.insert(p_edge_11_sk.clone(), ovbs.clone());
                        println!("Embedded child nodes {}",cuids.len());

                        let mut dp_tasks = ovb_pk.len() + 1;
                        println!(" OvB ================== dp_tasks to wait for {}",ovbs.len());
                        let (btask_ch, mut block_task_rx) = tokio::sync::mpsc::channel::<bool>(4);

                        let block_task_ch = btask_ch.clone();
                        let dyn_client = m_dyn_client.clone();
                        let graph_sn = graph_sn.clone();
                        let retry_ch__ = retry_ch_.clone();
                        let node_types_2 = node_types.clone();
                        let ty_with_11 = ty_with_11_.clone();
                        //let p_edges_11 = p_edges_11.clone();
                        let sk_base = sk_base.clone();
                        let m_sk_base = sk_base.clone();
                        let p_edge_11_ = p_edge_11.clone();
                        let p_ty = p_ty_.clone();
                        let puid_ = puid.clone();
                        let p_edge_11_sk = p_edge_11_sk_.clone(); // Arc??
                        let ovb_pk_ = ovb_pk.clone(); // Arc it??

                        // let (cuids, ovbs, bids) =
                        //     fetch_child_nodes(&dyn_client, &puid, &p_edge_11_sk, table_name).await;
                        // let mut ovb_pk: HashMap<String, Vec<Uuid>> = HashMap::new();
                        // ovb_pk.insert(p_edge_11_sk.clone(), ovbs.clone());
                        // let ovb_pk_= ovb_pk.clone(); //TODO  Arc it

                        // tasks created for embedded and OVB so reading of cuids executed in parallel
                        // First, start a task for Embedded uuids.
                        tokio::spawn(async move {
                            // =================================================================================
                            // for each 1:1 edge in child (Actor,Genre,Character), query the scalar propagated data
                            // firstly in embedded and then in OvB. Maximum consumption in memory will be either
                            // EMBEDDED_CHILD_NODES or OV_MAX_BATCH_SIZE nodes.
                            // =================================================================================
                            let mut items: HashMap<String, DpPropagate> = HashMap::new();
                            let mut bat_w_req: Vec<WriteRequest> = vec![];

                            for c_uid in cuids {

                                println!(
                                    "----------- child uuid [{}] ---------",
                                    c_uid.to_string()
                                );
                                // note: actual scalar query not performed on node containing scalar data but on propagated data contained
                                // in immediate child of parent node, hence the use of edge sortk based values.
                                // how many 11 edges  - basis of sortk for child query
                                let p_edge_11_c_ty = node_types_2.get(&p_edge_11_.ty); // e.g. Performance 1:1 edge - in parent Person
                                                                                       // 11 edges on child node (e.g. Performance) which must have 1:1 attributes, e.g. Actor,Genre,Character
                                let Some(pg_11_edges) = ty_with_11.get(&p_edge_11_c_ty) else {
                                    panic!(
                                        "logic error: expected type {:?} in ty_with_11",
                                        p_edge_11_c_ty
                                    )
                                };

                                // reverse edge item : R#<node-type-sn>#edge_sk
                                let r_target_sk: String = "R#".to_string() + p_ty.short_nm() + "#" + &p_edge_11_sk;
                                bat_w_req = add_reverse_edge(&dyn_client, bat_w_req, c_uid.clone(), puid_, r_target_sk.clone(),0,0, &retry_ch__, table_name).await;

                                // allocate node cache for one or more 1:1 edges in child node
                                let mut node_cache: types::NodeCache =
                                    types::NodeCache(HashMap::new());
                                // ===========================================================
                                // query each 1:1 child edge [Character, Genre, Actor]
                                // ===========================================================
                                for pg_11_edge_ in pg_11_edges {
                                    let pg_11_edge = pg_11_edge_.clone();

                                    let pg_11_edge_cache_key =
                                        sk_base.clone() + pg_11_edge.c.as_str();
                                    println!(
                                        "========== pg_11_edge [{}] ==========",
                                        pg_11_edge_cache_key
                                    );

                                    if !node_cache.0.contains_key(&pg_11_edge_cache_key) {
                                        // ===========================================================
                                        // populate node cache using one or more sortk queries.
                                        // ===========================================================
                                        // total edges on child type
                                        let c_all_edges = p_edge_11_c_ty.edge_cnt();
                                        // 11 edges only
                                        let pg_11_edges_cnt = pg_11_edges.len();
                                        // populate c_sk_query with a general edge value of specific edge
                                        //let c_sk_query=vec![sk_base.clone() + pg_11_edge.c.as_str()];
                                        // below will handle case of many 11 edges on child - reading in multiple edges - maybe not a good idea
                                        // given each edge might contain a lot of data however query will only read edge item not Ovb
                                        let c_sk_query = if pg_11_edges_cnt == 1 {
                                            vec![sk_base.clone() + pg_11_edge.c.as_str()]
                                        } else {
                                            match c_all_edges
                                                > pg_11_edges_cnt * EDGE_MULTIPLY_THRESHOLD
                                            {
                                                true => {
                                                    // use multiple specific SKs
                                                    let mut v_sk = vec![];
                                                    for e in pg_11_edges.into_iter() {
                                                        v_sk.push(sk_base.clone() + e.c.as_str())
                                                    }
                                                    v_sk
                                                }
                                                false => vec![sk_base.clone()], // global edge sk query "m|A#G#" - contains propagated scalar data
                                            }
                                        };

                                        println!("c_sk_query [{:?}]", c_sk_query);
                                        for c_sk in &c_sk_query {
                                            println!("c_sk [{}]", &c_sk);

                                            // perform query on cuid, query_sk
                                            let result = dyn_client
                                                .query()
                                                .table_name(table_name)
                                                .key_condition_expression(
                                                    "#p = :uid and begins_with(#s,:sk_v)",
                                                )
                                                .expression_attribute_names("#p", types::PK)
                                                .expression_attribute_names("#s", types::SK)
                                                .expression_attribute_values(
                                                    ":uid",
                                                    AttributeValue::B(Blob::new(c_uid.as_bytes())),
                                                )
                                                .expression_attribute_values(
                                                    ":sk_v",
                                                    AttributeValue::S(c_sk.clone()),
                                                )
                                                .send()
                                                .await;

                                            if let Err(err) = result {
                                                panic!("error in query() {}", err);
                                            }
                                            let mut q: Vec<types::DataItem> = vec![];

                                            if let Some(items) = result.unwrap().items {
                                                q = items.into_iter().map(|v| v.into()).collect();
                                            }

                                            for c in q {
                                                node_cache.0.insert(c.sk.0.clone(), c);
                                            }
                                        }
                                        for (k, v) in &node_cache.0 {
                                            println!("** cache: key [{}]  {:?}", k, v.sk)
                                        }
                                    }

                                    let nt = node_types_2.get(&pg_11_edge.ty);
                                    println!(" nt = [{}]", nt.long_nm());
                                    let pg_11_edge_scalars = nt.get_scalars_flatten();
                                    println!(" pg_11_edge_scalars = [{:?}]", pg_11_edge_scalars);

                                    // double propagate Scalars
                                    // source from propagated scalar data in child node (as produced by "sp", single-propagation process)
                                    for scalar in pg_11_edge_scalars {
                                        // cached sk
                                        let mut cpg_sk =
                                            sk_base.clone() + &pg_11_edge.c[..] + "#:" + &scalar.c;
                                        // sk_base.clone() + edge_11_sn + "#:" + &scalar.short_nm();
                                        println!("=== scalars : {}", cpg_sk);

                                        let di = match node_cache.0.remove(&cpg_sk[..]) {
                                            Some(di) => di,
                                            None => types::DataItem::new(),
                                        };

                                        let mut pg_sk = p_edge_11_sk.clone()
                                            + "#:"
                                            + &pg_11_edge.c[..]
                                            + "#:"
                                            + &scalar.c;

                                        match scalar.dt.as_str() {
                                                    // edge predicate (uuidpred)
                                                    "I" | "F" => match di.ln {
                                                        Some(mut vv) => match vv.remove(0) {
                                                            Some(s) => match items.get_mut(&pg_sk) {
                                                                None => {
                                                                    items.insert(
                                                                        pg_sk.clone(),
                                                                        DpPropagate {
                                                                            entry: Some(LN),
                                                                            psk: p_edge_11_sk.clone(),
                                                                            nd: vec![],
                                                                            ls: vec![],
                                                                            ln: vec![AttributeValue::N(s)],
                                                                            lbl: vec![],
                                                                            lb: vec![],
                                                                            ldt: vec![],
                                                                        },
                                                                    );
                                                                },
                                                                Some(v) =>  v.ln.push(AttributeValue::N(s)),                                                           },
                                                            _ => panic!(
                                                                "expected Some got None in cache for I or F type"
                                                            ),
                                                        },
                                                        _ => {
                                                            panic!(
                                                                "Data Error: Attribute {} in type {} has no value",
                                                                pg_11_edge.c,
                                                                p_edge_11_c_ty.long_nm()
                                                            )
                                                        },
                                                    },
                                                    "S" => {
                                                        match di.ls {
                                                            Some(mut vv) => match vv.remove(0) {
                                                                Some(s) => match items.get_mut(&pg_sk) {
                                                                    None => {
                                                                        items.insert(
                                                                            pg_sk.clone(),
                                                                            DpPropagate {
                                                                                entry:  Some(LS),
                                                                                psk: p_edge_11_sk.clone(),
                                                                                nd: vec![],
                                                                                ls: vec![AttributeValue::S(s)],
                                                                                ln: vec![],
                                                                                lbl: vec![],
                                                                                lb: vec![],
                                                                                ldt: vec![],
                                                                            },
                                                                        );
                                                                    }
                                                                    Some(v) => v.ls.push(AttributeValue::S(s)),
                                                                },
                                                                _ => panic!(
                                                                    "expected Some got None in cache for Stype"
                                                                ),
                                                            },
                                                            _ => {
                                                                println!("Data Error: Attribute {} in type {} has no value",pg_11_edge.c,p_edge_11_c_ty.long_nm())
                                                            },
                                                        }
                                                    }
                                                    "Bl" => {
                                                        match di.lbl {
                                                            Some(vv) => match vv[0] {
                                                                Some(bl) => match items.get_mut(&pg_sk) {
                                                                    None => {
                                                                        items.insert(
                                                                            pg_sk.clone(),
                                                                            DpPropagate {
                                                                                entry: Some(LBL),
                                                                                psk: p_edge_11_sk.clone(),
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
                                                                _ => panic!(
                                                                    "expected Some got None in cache for Bl type"
                                                                ),
                                                            },
                                                            _ => {
                                                                println!("Data Error: Attribute {} in type {} has no value",pg_11_edge.c,p_edge_11_c_ty.long_nm())
                                                            },
                                                        }
                                                    }
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
                                                                                        entry: Some(LB),
                                                                                        psk: p_edge_11_sk.clone(),
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
                                                                println!("Data Error: Attribute {} in type {} has no value",pg_11_edge.c,p_edge_11_c_ty.long_nm())
                                                            }
                                                        }
                                                    }
                                                    "Nd" => {}
                                                    _ => {
                                                        panic!(
                                                            "Unhandled match: got {} in match of dt",
                                                            pg_11_edge.dt.as_str()
                                                        )
                                                    }
                                        } // match
                                    }
                                }
                            }
                            // ===================================================================
                            // 9.2.3 for all EMBEDDED child nodes persist scalars for each 11 edge 
                            // ===================================================================
                            println!(" PERSIST embedded ");
                            persist(
                                    &dyn_client,
                                    bat_w_req, 
                                    puid_,
                                    &retry_ch__,
                                    &ovb_pk, //&ovb_pk, 
                                    items,
                                    table_name,
                                    false,
                            )
                            .await;

                            if let Err(e) = block_task_ch.send(true).await {
                                panic!("error sending on channel task_ch - {}", e);
                            }
                        });

                        // handle OvB (Overflow Blocks) reading each batch of Uuids in each OvB
                        // Persist to database each batch individually.
                        for (i, ouid) in ovbs.into_iter().enumerate() {

                            println!(" OvB now for OUID {}  [{}]",i, ouid.to_string());

                            let block_task_ch = btask_ch.clone();
                            let dyn_client = m_dyn_client.clone();
                            let graph_sn = graph_sn.clone();
                            let retry_ch__ = retry_ch_.clone();
                            let node_types_3 = node_types.clone();
                            let ty_with_11 = ty_with_11_.clone();
                            //let p_edges_11 = p_edges_11.clone();
                            let sk_base = m_sk_base.clone();
                            let p_edge_11 = p_edge_11.clone();
                            let ovb_pk = ovb_pk_.clone(); // Arc it??
                            let p_edge_11_sk = p_edge_11_sk_.clone();
                            //let retry_ch_ = retry_ch_.clone();

                            // max batch id in current OvB
                            let bid = bids[i];

                            // spawn a task for each OvB and process each batch
                            println!("spawn for OvB bid: {}",bid);
                            tokio::spawn(async move {
                                let mut id = 1;
                                //let mut bat_w_req: Vec<WriteRequest> = vec![];
                                // process cuids in each OvB batch
                                while id <= bid {

                                    // destination SK to persist 
                                    let batch_sk = p_edge_11_sk.clone() + "%" + &id.to_string();

                                    id += 1;

                                    let cuids = fetch_child_nodes_ovb(
                                        &dyn_client,
                                        &ouid,
                                        &batch_sk,
                                        table_name,
                                    )
                                    .await;
                                    println!("OvB child uuids in batch {} {}",id-1, cuids.len());
                                    // ===================================================================================
                                    // for each 1:1 edge in current child (Actor,Genre,Character), query the scalar propagated data
                                    // ===================================================================================
                                    let mut items: HashMap<String, DpPropagate> = HashMap::new();
                                    for c_uid in &cuids {
                                        println!(
                                            "----------- ovb uuid [{}] ---------",
                                            c_uid.to_string()
                                        );
                                        // note: actual scalar query not performed on node containing scalar data but on propagated data contained
                                        // in immediate child of parent node, hence the use of edge sortk based values.
                                        // how many 11 edges  - basis of sortk for child query
                                        let p_edge_11_c_ty = node_types_3.get(&p_edge_11.ty); // e.g. Performance 1:1 edge - in parent Person
                                                                                              // 11 edges on child node (e.g. Performance) which must have 1:1 attributes, e.g. Actor,Genre,Character
                                        let Some(pg_11_edges) = ty_with_11.get(&p_edge_11_c_ty)
                                        else {
                                            panic!(
                                                "logic error: expected type {:?} in ty_with_11",
                                                p_edge_11_c_ty
                                            )
                                        };

                                        // allocate node cache for 1:1 edges in child node
                                        let mut node_cache: types::NodeCache =
                                            types::NodeCache(HashMap::new());
                                        // ===========================================================
                                        // query each 1:1 child edge [Character, Genre, Actor]
                                        // ===========================================================
                                        println!("OvB pg_11_edges len {}", pg_11_edges.len());
                                        for pg_11_edge_ in pg_11_edges {
                                            let pg_11_edge = pg_11_edge_.clone();

                                            let pg_11_edge_cache_key =
                                                sk_base.clone() + pg_11_edge.c.as_str();
                                            println!(
                                                "OvB ========== pg_11_edge_ [{}] ==========",
                                                pg_11_edge_cache_key
                                            );

                                            // is edge data in cache from previous generalised load?
                                            if !node_cache.0.contains_key(&pg_11_edge_cache_key) {
                                                // ===========================================================
                                                // populate node cache using one or more sortk queries.
                                                // ===========================================================
                                                // total edges on child type
                                                let c_all_edges = p_edge_11_c_ty.edge_cnt();
                                                // 11 edges only
                                                let pg_11_edges_cnt = pg_11_edges.len();
                                                // populate c_sk_query with a general edge value of specific edge
                                                // let c_sk_query=vec![sk_base.clone() + pg_11_edge.c.as_str()];
                                                // below will handle case of many 11 edges on child - reading in multiple edges - maybe not a good idea
                                                // given each edge might contain a lot of data however query will only read edge item not Ovb
                                                let c_sk_query = if pg_11_edges_cnt == 1 {
                                                    vec![sk_base.clone() + pg_11_edge.c.as_str()]
                                                } else {
                                                    match c_all_edges
                                                        > pg_11_edges_cnt * EDGE_MULTIPLY_THRESHOLD
                                                    {
                                                        true => {
                                                            // use multiple specific SKs
                                                            let mut v_sk = vec![];
                                                            for e in pg_11_edges {
                                                                v_sk.push(
                                                                    sk_base.clone() + e.c.as_str(),
                                                                )
                                                            }
                                                            v_sk
                                                        }
                                                        false => vec![sk_base.clone()], // global edge sk query "m|A#G#" - contains propagated scalar data
                                                    }
                                                };

                                                println!("OvB - c_sk_query [{:?}]", c_sk_query);
                                                for c_sk in &c_sk_query {
                                                    println!("c_sk [{}]", &c_sk);

                                                    // perform query on cuid, query_sk
                                                    let result = dyn_client
                                                        .query()
                                                        .table_name(table_name)
                                                        .key_condition_expression(
                                                            "#p = :uid and begins_with(#s,:sk_v)",
                                                        )
                                                        .expression_attribute_names("#p", types::PK)
                                                        .expression_attribute_names("#s", types::SK)
                                                        .expression_attribute_values(
                                                            ":uid",
                                                            AttributeValue::B(Blob::new(
                                                                c_uid.as_bytes(),
                                                            )),
                                                        )
                                                        .expression_attribute_values(
                                                            ":sk_v",
                                                            AttributeValue::S(c_sk.clone()),
                                                        )
                                                        .send()
                                                        .await;

                                                    if let Err(err) = result {
                                                        panic!("error in query() {}", err);
                                                    }
                                                    let mut q: Vec<types::DataItem> = vec![];

                                                    if let Some(items) = result.unwrap().items {
                                                        q = items
                                                            .into_iter()
                                                            .map(|v| v.into())
                                                            .collect();
                                                    }

                                                    for c in q {
                                                        node_cache.0.insert(c.sk.0.clone(), c);
                                                    }
                                                }
                                                for (k, v) in &node_cache.0 {
                                                    println!("** cache: key [{}]  {:?}", k, v.sk)
                                                }
                                            }

                                            // child node type for current edge
                                            let nt = node_types_3.get(&pg_11_edge.ty);
                                            println!(" nt = [{}]", nt.long_nm());
                                            let pg_11_edge_scalars = nt.get_scalars_flatten();
                                            println!(
                                                " pg_11_edge_scalars = [{:?}]",
                                                pg_11_edge_scalars
                                            );

                                            // propagate propagated Scalars for current edge on child
                                            for scalar in pg_11_edge_scalars {
                                                // cached sk
                                                let mut cpg_sk = sk_base.clone()
                                                    + &pg_11_edge.c[..]
                                                    + "#:"
                                                    + &scalar.c;
                                                // sk_base.clone() + edge_11_sn + "#:" + &scalar.short_nm();
                                                println!("Ovb === scalars : {}", cpg_sk);

                                                let di = match node_cache.0.remove(&cpg_sk[..]) {
                                                    Some(di) => di,
                                                    None => types::DataItem::new(),
                                                };
                                                let mut pg_sk = batch_sk.clone()
                                                    + "#:"
                                                    + &pg_11_edge.c[..]
                                                    + "#:"
                                                    + &scalar.c;
                                                // let Some(di) = node_cache.0.remove(&pg_sk[..]) else {
                                                //     panic!("Logic error: not found in node_cache [{}]", pg_sk);
                                                // };

                                                match scalar.dt.as_str() {
                                                                // edge predicate (uuidpred)
                                                                "I" | "F" => match di.ln {
                                                                    Some(mut vv) => match vv.remove(0) {
                                                                        Some(s) => match items.get_mut(&pg_sk) {
                                                                            None => {
                                                                                items.insert(
                                                                                    pg_sk.clone(),
                                                                                    DpPropagate {
                                                                                        entry: Some(LN),
                                                                                        psk: p_edge_11_sk.clone(),
                                                                                        nd: vec![],
                                                                                        ls: vec![],
                                                                                        ln: vec![AttributeValue::N(s)],
                                                                                        lbl: vec![],
                                                                                        lb: vec![],
                                                                                        ldt: vec![],
                                                                                    },
                                                                                );
                                                                            },
                                                                            Some(v) =>  v.ln.push(AttributeValue::N(s)),
                                                                        },
                                                                        _ => panic!(
                                                                            "expected Some got None in cache for I or F type"
                                                                        ),
                                                                    },
                                                                    _ => {
                                                                        panic!(
                                                                            "Data Error: Attribute {} in type {} has no value",
                                                                            pg_11_edge.c,
                                                                            p_edge_11_c_ty.long_nm()
                                                                        )
                                                                    },
                                                                },
                                                                "S" => {
                                                                    match di.ls {
                                                                        Some(mut vv) => match vv.remove(0) {
                                                                            Some(s) => match items.get_mut(&pg_sk) {
                                                                                None => {
                                                                                    items.insert(
                                                                                        pg_sk.clone(),
                                                                                        DpPropagate {
                                                                                            entry:  Some(LS),
                                                                                            psk: p_edge_11_sk.clone(),
                                                                                            nd: vec![],
                                                                                            ls: vec![AttributeValue::S(s)],
                                                                                            ln: vec![],
                                                                                            lbl: vec![],
                                                                                            lb: vec![],
                                                                                            ldt: vec![],
                                                                                        },
                                                                                    );
                                                                                }
                                                                                Some(v) => v.ls.push(AttributeValue::S(s)),
                                                                            },
                                                                            _ => panic!(
                                                                                "expected Some got None in cache for Stype"
                                                                            ),
                                                                        },
                                                                        _ => {
                                                                            println!("Data Error: Attribute {} in type {} has no value",pg_11_edge.c,p_edge_11_c_ty.long_nm())
                                                                        },
                                                                    }
                                                                }
                                                                "Bl" => {
                                                                    match di.lbl {
                                                                        Some(vv) => match vv[0] {
                                                                            Some(bl) => match items.get_mut(&pg_sk) {
                                                                                None => {
                                                                                    items.insert(
                                                                                        pg_sk.clone(),
                                                                                        DpPropagate {
                                                                                            entry: Some(LBL),
                                                                                            psk: p_edge_11_sk.clone(),
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
                                                                            _ => panic!(
                                                                                "expected Some got None in cache for Bl type"
                                                                            ),
                                                                        },
                                                                        _ => {
                                                                            println!("Data Error: Attribute {} in type {} has no value",pg_11_edge.c,p_edge_11_c_ty.long_nm())
                                                                        },
                                                                    }
                                                                }
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
                                                                                                    entry: Some(LB),
                                                                                                    psk: p_edge_11_sk.clone(),
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
                                                                            println!("Data Error: Attribute {} in type {} has no value",pg_11_edge.c,p_edge_11_c_ty.long_nm())
                                                                        }
                                                                    }
                                                                }
                                                                "Nd" => {}
                                                                _ => {
                                                                    panic!(
                                                                        "Unhandled match: got {} in match of dt",
                                                                        pg_11_edge.dt.as_str()
                                                                    )
                                                                }
                                                    } // match
                                            }
                                        }
                                    }
                                    // ================================================================================
                                    // 9.2.3 for each 11 edge in child node persist OvB batch worth of propagated scalar data
                                    // ================================================================================
                                    println!("PERSIST OvB .......items {}",items.len());
                                    for (k,v) in &ovb_pk {
                                        println!("ovb_pk {}  {:?}",k,v);
                                    }
                                    persist(
                                        &dyn_client,
                                        vec![], 
                                        ouid.clone(), // TODO: Arc it??
                                        &retry_ch__,
                                        &ovb_pk,
                                        items,
                                        table_name,
                                        true
                                    )
                                    .await;

                                }

                                if let Err(e) = block_task_ch.send(true).await {
                                    panic!("error sending on channel task_ch - {}", e);
                                }
                            });
                        }
                        // ============================================================
                        // 10.0 Wait for child processing across embedded and Ov blocks
                        // ============================================================
                        while dp_tasks > 0 {
                            // wait for a task to finish...
                            block_task_rx.recv().await;
                            dp_tasks -= 1;
                        }
                    }
                    // concurrent task counter
                    node_tasks += 1;
                    // ====================================
                    // 9.2.4 send complete message to main
                    // ====================================
                    if let Err(e) = task_ch.send(true).await {
                        panic!("error sending on channel task_ch - {}", e);
                    }
                });
                // =============================================================
                // 9.3 Wait for task to complete if max concurrent tasks reached
                // =============================================================
                if node_tasks == MAX_TASKS {
                    // wait for a task to finish...
                    node_task_rx.recv().await;
                    node_tasks -= 1;
                }
            }
        }
    }
    // =========================================
    // 10.0 Wait for remaining tasks to complete
    // =========================================
    while node_tasks > 0 {
        // wait for a task to finish...
        node_task_rx.recv().await;
        node_tasks -= 1;
    }

    println!("Waiting for support services to finish...");
    shutdown_broadcast_ch.send(0);
    retry_service.await;

    Ok(())
}

async fn add_reverse_edge(
    dyn_client: &aws_sdk_dynamodb::Client,
    bat_w_req : Vec<WriteRequest>,
    child: Uuid,
    target_uid: Uuid,
    target_sk : String,
    batch : i32,
    id : i32,
    retry_ch: &tokio::sync::mpsc::Sender<Vec<aws_sdk_dynamodb::types::WriteRequest>>,
    table_name : &str,
) -> Vec<WriteRequest> {

    let put = aws_sdk_dynamodb::types::PutRequest::builder();
    let mut put = put
    .item(types::PK, AttributeValue::B(Blob::new(child)))
    .item(types::SK, AttributeValue::S(target_sk))
    .item(types::TARGET, AttributeValue::B(Blob::new(target_uid)))
    .item(types::BID, AttributeValue::N(batch.to_string()))
    .item(types::ID, AttributeValue::N(id.to_string()));   
    
    save_item(&dyn_client, bat_w_req, retry_ch, put, table_name).await

}    

async fn persist(
    dyn_client: &aws_sdk_dynamodb::Client,
    mut bat_w_req: Vec<WriteRequest>,
    uid: Uuid,
    retry_ch: &tokio::sync::mpsc::Sender<Vec<aws_sdk_dynamodb::types::WriteRequest>>,
    ovb_pk: &HashMap<String, Vec<Uuid>>,
    items: HashMap<SortK, DpPropagate>,
    table_name: &str,
    ovb : bool,
) {
    // persist to database
    //let mut bat_w_req: Vec<WriteRequest> = vec![];
    println!("============ Persist ============= {})",items.len());
    for (sk, mut e) in items {
        println!(
            "Persist for UID [{}] [{:?}]  sk  [{}]",
            uid.to_string(),
            uid.as_bytes(),
            sk
        );

        //let ovbs : Vec<AttributeValue> = ovb_pk.get(&e.psk).unwrap().iter().map(|uid| AttributeValue::B(Blob::new(uid.clone().as_bytes()))).collect();

        let mut finished = false;

        let put = aws_sdk_dynamodb::types::PutRequest::builder();
        let mut put = put
            .item(types::PK, AttributeValue::B(Blob::new(uid.clone())))
            .item(types::SK, AttributeValue::S(sk.clone()));
        // add OVB attribute to indicate if any ovbs are being used for this
        // particular parent edge in parent data block
        let mut put =  match ovb_pk.get(&e.psk) {
                None => {
                    panic!("Logic error: no key found in ovb_pk for {}", e.psk)
                }
                Some(v) if !ovb => match v.len() {
                    0 => put.item(types::OVB, AttributeValue::Bool(false)),
                    _ => put.item(types::OVB, AttributeValue::Bool(true)),
                }
                Some(v) => put,
            };

        match e.entry.unwrap() {
            LS => {
                if e.ls.len() <= EMBEDDED_CHILD_NODES {
                    let embedded: Vec<_> = e.ls.drain(..e.ls.len()).collect();
                    put = put.item(types::LS, AttributeValue::L(embedded));
                    finished = true;
                } else {
                    let embedded: Vec<_> = e.ls.drain(..EMBEDDED_CHILD_NODES).collect();
                    put = put.item(types::LS, AttributeValue::L(embedded));
                }
            }
            LN => {
                if e.ln.len() <= EMBEDDED_CHILD_NODES {
                    let embedded: Vec<_> = e.ln.drain(..e.ln.len()).collect();
                    put = put.item(types::LN, AttributeValue::L(embedded));
                    finished = true;
                } else {
                    let embedded: Vec<_> = e.ln.drain(..EMBEDDED_CHILD_NODES).collect();
                    put = put.item(types::LN, AttributeValue::L(embedded));
                }
            }
            LBL => {
                if e.lbl.len() <= EMBEDDED_CHILD_NODES {
                    let embedded: Vec<_> = e.lbl.drain(..e.lbl.len()).collect();
                    put = put.item(types::LBL, AttributeValue::L(embedded));
                    finished = true;
                } else {
                    let embedded: Vec<_> = e.lbl.drain(..EMBEDDED_CHILD_NODES).collect();
                    put = put.item(types::LBL, AttributeValue::L(embedded));
                }
            }
            LB => {
                if e.lb.len() <= EMBEDDED_CHILD_NODES {
                    let embedded: Vec<_> = e.lb.drain(..e.lb.len()).collect();
                    put = put.item(types::LB, AttributeValue::L(embedded));
                    finished = true;
                } else {
                    let embedded: Vec<_> = e.lb.drain(..EMBEDDED_CHILD_NODES).collect();
                    put = put.item(types::LB, AttributeValue::L(embedded));
                }
            }
            _ => {
                panic!("unexpected entry match in Operation::Propagate")
            }
        };

        //println!("save_item...{}",bat_w_req.len());
        bat_w_req = save_item(&dyn_client, bat_w_req, retry_ch, put, table_name).await;

        if finished {
            println!("** finished...continue");
            continue;
        }

        // =========================================
        // add batches across ovbs until max reached
        // =========================================
        let mut bid = 0;
        for ovb in ovb_pk.get(&e.psk).unwrap() {
            bid = 0;

            while bid < OV_BATCH_THRESHOLD && !finished {
                bid += 1;
                let mut sk_w_bid = sk.clone();
                sk_w_bid.push('%');
                sk_w_bid.push_str(&bid.to_string());

                let put = aws_sdk_dynamodb::types::PutRequest::builder();
                let mut put = put
                    .item(types::PK, AttributeValue::B(Blob::new(ovb.clone())))
                    .item(types::SK, AttributeValue::S(sk_w_bid));

                match e.entry.unwrap() {
                    LS => {
                        if e.ls.len() <= OV_MAX_BATCH_SIZE {
                            let batch: Vec<_> = e.ls.drain(..e.ls.len()).collect();
                            put = put.item(types::LS, AttributeValue::L(batch));
                            finished = true;
                        } else {
                            let batch: Vec<_> = e.ls.drain(..OV_MAX_BATCH_SIZE).collect();
                            put = put.item(types::LS, AttributeValue::L(batch));
                        }
                    }

                    LN => {
                        if e.ln.len() <= OV_MAX_BATCH_SIZE {
                            let batch: Vec<_> = e.ln.drain(..e.ln.len()).collect();
                            put = put.item(types::LN, AttributeValue::L(batch));
                            finished = true;
                        } else {
                            let batch: Vec<_> = e.ln.drain(..OV_MAX_BATCH_SIZE).collect();
                            put = put.item(types::LN, AttributeValue::L(batch));
                        }
                    }

                    LBL => {
                        if e.lbl.len() <= OV_MAX_BATCH_SIZE {
                            let batch: Vec<_> = e.lbl.drain(..e.lbl.len()).collect();
                            put = put.item(types::LBL, AttributeValue::L(batch));
                            finished = true;
                        } else {
                            let batch: Vec<_> = e.lbl.drain(..OV_MAX_BATCH_SIZE).collect();
                            put = put.item(types::LBL, AttributeValue::L(batch));
                        }
                    }

                    LB => {
                        if e.lb.len() <= OV_MAX_BATCH_SIZE {
                            let batch: Vec<_> = e.lb.drain(..e.lb.len()).collect();
                            put = put.item(types::LB, AttributeValue::L(batch));
                            finished = true;
                        } else {
                            let batch: Vec<_> = e.lb.drain(..OV_MAX_BATCH_SIZE).collect();
                            put = put.item(types::LB, AttributeValue::L(batch));
                        }
                    }
                    _ => {
                        panic!("unexpected entry match in Operation::Propagate")
                    }
                }

                bat_w_req = save_item(&dyn_client, bat_w_req, retry_ch, put, table_name).await;
            }
            if finished {
                break;
            }
        }
        // =============================================
        // keep adding batches across ovbs (round robin)
        // =============================================
        while !finished {
            bid += 1;

            for ovb in ovb_pk.get(&e.psk).unwrap() {
                let mut sk_w_bid = sk.clone();
                sk_w_bid.push('%');
                sk_w_bid.push_str(&bid.to_string());
                let put = aws_sdk_dynamodb::types::PutRequest::builder();
                let mut put = put
                    .item(types::PK, AttributeValue::B(Blob::new(ovb.clone())))
                    .item(types::SK, AttributeValue::S(sk_w_bid));

                match e.entry.unwrap() {
                    LS => {
                        if e.ls.len() <= OV_MAX_BATCH_SIZE {
                            let batch: Vec<_> = e.ls.drain(..e.ls.len()).collect();
                            put = put.item(types::LS, AttributeValue::L(batch));
                            finished = true;
                        } else {
                            let batch: Vec<_> = e.ls.drain(..OV_MAX_BATCH_SIZE).collect();
                            put = put.item(types::LS, AttributeValue::L(batch));
                        }
                    }

                    LN => {
                        if e.ln.len() <= OV_MAX_BATCH_SIZE {
                            let batch: Vec<_> = e.ln.drain(..e.ln.len()).collect();
                            put = put.item(types::LN, AttributeValue::L(batch));
                            finished = true;
                        } else {
                            let batch: Vec<_> = e.ln.drain(..OV_MAX_BATCH_SIZE).collect();
                            put = put.item(types::LN, AttributeValue::L(batch));
                        }
                    }

                    LBL => {
                        if e.lbl.len() <= OV_MAX_BATCH_SIZE {
                            let batch: Vec<_> = e.ln.drain(..e.lbl.len()).collect();
                            put = put.item(types::LBL, AttributeValue::L(batch));
                            finished = true;
                        } else {
                            let batch: Vec<_> = e.ln.drain(..OV_MAX_BATCH_SIZE).collect();
                            put = put.item(types::LBL, AttributeValue::L(batch));
                        }
                    }

                    LB => {
                        if e.lb.len() <= OV_MAX_BATCH_SIZE {
                            let batch: Vec<_> = e.lb.drain(..e.lb.len()).collect();
                            put = put.item(types::LB, AttributeValue::L(batch));
                            finished = true;
                        } else {
                            let batch: Vec<_> = e.lb.drain(..OV_MAX_BATCH_SIZE).collect();
                            put = put.item(types::LB, AttributeValue::L(batch));
                        }
                    }
                    _ => {
                        panic!("unexpected entry match in Operation::Propagate")
                    }
                }

                bat_w_req = save_item(&dyn_client, bat_w_req, retry_ch, put, table_name).await;

                if e.ls.len() == 0 && e.ln.len() == 0 && e.ln.len() == 0 && e.lb.len() == 0 {
                    finished = true;
                    break;
                }
            }
        }

        if bat_w_req.len() > 0 {
            //print_batch(bat_w_req);
            println!("last chance....");
            bat_w_req = persist_dynamo_batch(dyn_client, bat_w_req, retry_ch, table_name).await;
        }
    } // end for

    if bat_w_req.len() > 0 {
        //print_batch(bat_w_req);
        println!("last chance....2");
        bat_w_req = persist_dynamo_batch(dyn_client, bat_w_req, retry_ch, table_name).await;
    }
}

// returns node type as String, moving ownership from AttributeValue - preventing further allocation.
async fn fetch_child_nodes(
    dyn_client: &DynamoClient,
    uid: &Uuid,
    edge_sk: &str,
    table_name: impl Into<String>,
) -> (Vec<Uuid>, Vec<Uuid>, Vec<i32>) {
    let proj = types::ND.to_owned() + "," + types::XF + "," + types::BID + "," + types::TY;

    let result = dyn_client
        .get_item()
        .table_name(table_name)
        .key(types::PK, AttributeValue::B(Blob::new(uid.clone())))
        .key(types::SK, AttributeValue::S(edge_sk.to_owned()))
        .projection_expression(proj)
        .send()
        .await;

    if let Err(err) = result {
        panic!(
            "get node type: no item found: expected a type value for node. Error: {}",
            err
        )
    }
    //let mut node : types::DataItem  = match result.unwrap().item {
    let mut node: types::DataItem = match result.unwrap().item {
        None => panic!(
            "No type item found in fetch_node_type() for [{}] [{}]",
            uid, edge_sk
        ),
        Some(v) => v.into(),
    };
    // Ty prefixed with "graph_sn|"", remove
    //let ty_ = (&node.ty.unwrap().find('|').unwrap() + 1..]).to_owned();

    let ovb_start_idx = node
        .xf
        .as_ref()
        .expect("di.xf contained none")
        .iter()
        .filter(|&&v| v < 4)
        .fold(0, |a, _| a + 1); // xf idx entry of first Ovb Uuid
    if ovb_start_idx > EMBEDDED_CHILD_NODES {
        panic!(
            "OvB inconsistency: XF embedded entry {} does not match EMBEDDED_CHILD_NODES {}",
            ovb_start_idx, EMBEDDED_CHILD_NODES
        );
    }

    let ovb_cnt = node
        .xf
        .expect("di.xf contained none")
        .iter()
        .filter(|&&v| v == 4)
        .fold(0, |a, _| a + 1);
    if ovb_cnt > MAX_OV_BLOCKS {
        panic!(
            "OvB inconsistency: XF attribute contains {} entry, MAX_OV_BLOCKS is {}",
            ovb_cnt, MAX_OV_BLOCKS
        );
    }
    let cuid: Vec<Uuid> = node
        .nd
        .as_mut()
        .expect("di.nd contained none")
        .drain(..ovb_start_idx)
        .collect();

    let ovb_pk: Vec<Uuid> = node
        .nd
        .as_mut()
        .expect("di.nd contained none")
        .drain(..)
        .collect();

    let bid = node
        .bid
        .as_mut()
        .expect("di.bid contained none")
        .drain(ovb_start_idx..)
        .collect();

    (cuid, ovb_pk, bid)
}

async fn fetch_child_nodes_ovb(
    dyn_client: &DynamoClient,
    ouid: &Uuid,
    edge_sk: &str, // includes batch id
    table_name: impl Into<String>,
) -> Vec<Uuid> {
    let proj = types::ND.to_owned() + "," + types::XF;

    let result = dyn_client
        .get_item()
        .table_name(table_name)
        .key(types::PK, AttributeValue::B(Blob::new(ouid.clone())))
        .key(types::SK, AttributeValue::S(edge_sk.to_owned()))
        .projection_expression(proj)
        .send()
        .await;

    if let Err(err) = result {
        panic!(
            "get node type: no item found: expected a type value for node. Error: {}",
            err
        )
    }

    let mut node: types::DataItem = match result.unwrap().item {
        None => panic!(
            "No type item found in fetch_node_type() for [{}] [{}]",
            ouid, edge_sk
        ),
        Some(v) => v.into(),
    };
    // remove deleted nodes from nd
    let mut nd = node.nd.expect("Node nd has values");

    node.xf
        .expect("Expect Xf Some")
        .into_iter()
        .enumerate()
        .for_each(|(i, v)| {
            if v == types::CHILD_DETACHED {
                nd.remove(i);
            }
        });

    println!("fetch_child_nodes_ovb nd [{:?}]",nd);

    nd
}

async fn save_item(
    dyn_client: &DynamoClient,
    mut bat_w_req: Vec<WriteRequest>,
    retry_ch: &tokio::sync::mpsc::Sender<Vec<aws_sdk_dynamodb::types::WriteRequest>>,
    put: PutRequestBuilder,
    table_name: &str,
) -> Vec<WriteRequest> {
    match put.build() {
        Err(err) => {
            println!("error in write_request builder: {}", err);
        }
        Ok(req) => {
            bat_w_req.push(WriteRequest::builder().put_request(req).build());
        }
    }
    //bat_w_req = print_batch(bat_w_req);

    if bat_w_req.len() == DYNAMO_BATCH_SIZE {
        // =================================================================================
        // persist to Dynamodb
        bat_w_req = persist_dynamo_batch(dyn_client, bat_w_req, &retry_ch, table_name).await;
        // =================================================================================
        //bat_w_req = print_batch(bat_w_req);
    }
    bat_w_req
}

async fn persist_dynamo_batch(
    dyn_client: &DynamoClient,
    bat_w_req: Vec<WriteRequest>,
    retry_ch: &tokio::sync::mpsc::Sender<Vec<aws_sdk_dynamodb::types::WriteRequest>>,
    table_name: &str,
) -> Vec<WriteRequest> {
    println!(
        "about to write batch [size {}] to dynamo...table {}",
        bat_w_req.len(),
        table_name
    );
    //bat_w_req.iter().map(|v|v.put_request.as_ref().clone().unwrap()).flat_map(|p|p.item).map(|(k,v)|println!("k,v {} {:?}",k,v));
    bat_w_req
        .iter()
        .map(|v| v.put_request.as_ref().expect("expect Some"))
        .map(|p| p.item.clone())
        .flatten()
        .for_each(|(k, v)| println!("k,v {} {:?}", k, v)); // works
       // bat_w_req.into_iter().map(|v|v.put_request.expect("expect Some")).map(|p|p.item).flatten().for_each(|(k,v)|println!("k,v {} {:?}",k,v)); // works but consumes

    //let Some((_,put_req_hm)) = bat_w_req.into_iter().take(1).map(|v| v.put_request.unwrap()).map(|v|Some(v.item)).enumerate().next() else {panic!("asdf")}; compiles
    //let Some(put_req_hm) = bat_w_req.into_iter().take(1).map(|v| v.put_request.unwrap()).map(|v|Some(v.item)).last() else {panic!("asdf")};

    let result = dyn_client
        .batch_write_item()
        .request_items(table_name, bat_w_req)
        .send()
        .await;

    match result {
        Err(err) => {
            panic!(
                "Error in Dynamodb batch write in persist_dynamo_batch() - {}",
                err
            );
        }
        Ok(resp) => {
            println!("successful put-item")

            // TODO: aggregate batchwrite metrics in bat_w_output.
            // pub item_collection_metrics: Option<HashMap<String, Vec<ItemCollectionMetrics>>>,
            // pub consumed_capacity: Option<Vec<ConsumedCapacity>>,
        }
    }
    let new_bat_w_req: Vec<WriteRequest> = vec![];

    new_bat_w_req
}

fn print_batch(bat_w_req: Vec<WriteRequest>) -> Vec<WriteRequest> {
    for r in bat_w_req {
        let WriteRequest {
            put_request: pr, ..
        } = r;
        println!(" ------------------------  ");
        for (attr, attrval) in pr.unwrap().item {
            // HashMap<String, AttributeValue>,
            println!(" putRequest [{}]   {:?}", attr, attrval);
        }
    }

    let new_bat_w_req: Vec<WriteRequest> = vec![];

    new_bat_w_req
}
