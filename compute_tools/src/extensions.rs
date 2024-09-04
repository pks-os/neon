use compute_api::responses::Extension;
use compute_api::responses::ExtenstionsList;
use url::Url;
use std::collections::HashMap;

// TODO limit the number of databases we query
// TODO write proper version comparison
//
// TODO call it from somewhere, i.e. from ControlPlane after StartCompute (?)
// and save it in some per-endpoint stat table in cplane


use postgres::{Client, NoTls};
use tokio::task;
use anyhow::Result;
use tracing::info;


pub fn list_dbs(client: &mut Client) -> Result<Vec<String>> {
    // `pg_database.datconnlimit = -2` means that the database is in the
    // invalid state
    let databases = client
        .query("SELECT datname FROM pg_database
                WHERE datallowconn
                AND datconnlimit <> - 2;", &[])?
        .iter()
        .map(|row| {
            let db: String = row.get("datname");
            db
        })
        .collect();

    Ok(databases)
}


pub async fn get_installed_extensions(connstr: Url) -> Result<ExtenstionsList> {

    let mut connstr = connstr.clone();

    task::spawn_blocking(move || {
        let mut client = Client::connect(connstr.as_str(), NoTls)?;
        let databases: Vec<String> = list_dbs(&mut client)?;

        let mut extensions_map: HashMap<String, Extension> = HashMap::new();
        for db in databases.iter() {
            connstr.set_path(&db);
            let mut db_client = Client::connect(connstr.as_str(), NoTls)?;
            let extensions: Vec<(String, String)> = db_client
                .query(
                    "SELECT extname, extversion FROM pg_catalog.pg_extension;",
                    &[],
                )?
                .iter()
                .map(|row| {
                    (row.get("extname"), row.get("extversion"))
                })
                .collect();
            
            for (extname, v) in extensions.iter() {
                // insert extension into the hashmap
                // update the highest_version if new version is higher
                // update the lowest_version if new version is lower
                let version  = v.to_string();
                extensions_map.entry(extname.to_string())
                    .and_modify(|e| {
                        // TODO write proper version comparison
                        if e.lowest_version > version {
                            e.lowest_version = version.clone();
                        }
                        if e.highest_version < version {
                            e.highest_version = version.clone();
                        }
                    })
                    .or_insert(Extension {
                        extname: extname.to_string(),
                        lowest_version: version.clone(),
                        highest_version: version.clone(),
                    });
            }

        }

        Ok(ExtenstionsList { extensions: extensions_map.values().cloned().collect() })
    })
    .await?

}


pub fn log_installed_extensions(connstr: Url) -> Result<()> {

    let connstr = connstr.clone();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to create rt");
    let result = rt.block_on(get_installed_extensions(connstr));

    info!("Installed extensions: {:?}", result);
    Ok(())
}