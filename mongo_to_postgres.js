//@ts-check

const { MongoClient } = require('mongodb');
const { createWriteStream, createReadStream } = require('fs');
const { Pool } = require('pg');

const projections = require("./projections.json");
const { postgres, mongo, batchType } = require("./config.json");

/**
 * Gets a Document and transforms it into a comma-separated row + a new-line character!
 *
 * Options:
 *
 *  - `doc` a document
 *
 * @param {Object} [doc]
 * @return {string}
 */
let objectToRow = doc => {
    let row = '';
    Object.keys(doc).forEach(element => {
        row = row + `'${doc[element]}'` + ','
    });
    return row.slice(0,-1) + '\n'
}

/**
 * Gets a Document and transforms it into a table definition of varchars
 *
 * Options:
 *
 *  - `doc` a document
 *
 * @param {Object} [doc]
 * @return {string}
 */
let objectToTable = doc => {
    let table = ``;
    Object.keys(doc).forEach(element => {
        if (doc[element]) {
            table = table + `${element} varchar(255),`
        }
    })
    return table.slice(0,-1);
}

let objectKeysToColumns = doc => {
    let row = '';
    Object.keys(doc).forEach(element => {
        row = row + `${element}` + ','
    });
    return row.slice(0,-1)
}

/**
 * Connects to MongoDB using a config and pre-stablished projections, returns files on disk and a Promise
 *
 * Options:
 *
 *  - `db` the db that you want to use
 *  - `collection` the collection that you want to get
 *  - `config` a config that is established on the config.json, to dump the collection wherever you want
 *
 * @param {Object} [db]
 * @param {Object} [collection]
 * @param {Object} [config]
 * @return {Promise}
 */
async function getFromMongo(db, collection, config) {
    
    // all the neded MongoDB clients and data
    const mongoClient = new MongoClient(mongo.uri, { useUnifiedTopology: true });
    await mongoClient.connect();
    
    // how many documents we will get? 
    let cap = mongo.cap;
    let totalDocuments = await mongoClient.db(db).collection(collection).countDocuments();
    
    // we use this limit so we can set the limit on the mongo query and not change it on the query itself
    let limit = (cap < totalDocuments) ? cap : totalDocuments
    
    let cursor = mongoClient.db(db).collection(collection).find({}).project(projections[collection]).limit(limit);
    
    // new pool of connections to postgreSQL
    const pool = new Pool(postgres)
    // we get the client in the pool
    const postgreClient = await pool.connect()

    //create table if not there
    let fields =  objectToTable(projections[collection]);
    // postgresql does not like tables with '-' characters, so we convert them to snake case. Also tables should have a 'd' before as they are a dump and should be recreated on each dump
    let sentence = `
    DROP TABLE IF EXISTS d_${collection.replace('-','_')};
    CREATE TABLE IF NOT EXISTS d_${collection.replace('-','_')} (${fields});`;
    await postgreClient.query(sentence); // something pending is to put the correct data types, here we are using varchars
    

    // a new File Stream in case we write to disk, we create it either way, since if not a file we end up with a file with 1 row
    let fileToWrite = createWriteStream(`${collection}.csv`)
    let header = projections[collection]
    delete header['_id']
    fileToWrite.write(objectKeysToColumns(header) + '\n')

    let i = 0; // a counter to get the status
    console.log(`${config} ${collection}`);
    
    return new Promise((resolve, reject) => {
        cursor
        .on('data', async chunk => {
            // we grab the whole projection to manipulate, since we will need to get the fields coming from Mongo and make them a table
            let objectToInsert = projections[collection];
            
            // Mongo can be a bit tricky and not send the whole document, so we will need to identify fields that are not coming and null them so we can make a row in postgres
            Object.keys(objectToInsert).forEach(element => {
                if (objectToInsert[element]) objectToInsert[element] = chunk[element] ? chunk[element] : null
                delete objectToInsert['_id'] // here I wipe the key _id that comes with every document in MongoDB, if you need it for any reason it can be added
            });

            if (config == 'toDB') {
                // here we make the prepared statement, with the document keys transformed into columns and the values of the document transformed into a row
                let insert = `INSERT INTO d_${collection.replace('-','_')} (${objectKeysToColumns(objectToInsert)}) VALUES (${objectToRow(objectToInsert)});`
                const res = await postgreClient.query(insert).catch(reject)
                i = i + res.rowCount;
                console.log(`Inserted ${i} of ${limit}`);
            }
            if (config == 'toDisk') {
                // if we insert this into a CSV file, we only insert the rows
                fileToWrite.write(objectToRow(objectToInsert));
                i++;
                console.log(`Wrote ${i} of ${limit}`);
            }

            if (i == limit) {
                // we make sure that the accumulator gets to the same level as the limit since otherwise we may cut the stream
                console.log(`Done ${collection}`);
                await postgreClient.release()
                await pool.end();
                
                await mongoClient.close();

                // fire resolve if we insert the rows to a DB, since the file only resolves if it ends writing
                if (config == 'toDB') resolve();
            }
        })
        .on('error', async error => {
            // if there's an error with the mongo stream we cut the streams and clients
            fileToWrite.end();
            console.log(`error on ${collection}, error details: ${error}`)
            await mongoClient.close();
            await postgreClient.release();
            await pool.end();
            reject(error);
        })
        
        fileToWrite.on('finish', async () => {
            console.log(`Done writing data from ${collection}`)
            fileToWrite.close();

            // fire resolve
            if(config == 'toDisk') resolve()
        })
        .on('error', async error => {
            console.log(`${error}`)
            fileToWrite.end();
            await mongoClient.close();
            await postgreClient.release();
            await pool.end();
            reject(error)
        })
    })
}

async function main() {
    let yourDB = 'aDB'
    // we loop through the projections to see which collections to get
    Object.keys(projections).forEach(async collection => {
        await getFromMongo(yourDB,collection, batchType.toDB)
        .catch(console.dir)
    })
}

main();