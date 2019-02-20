const MongoClient = require("mongodb").MongoClient
const log = require("../modules/logger")
const config = require("config")
const util = require("util")

const gnomedb = {}

/*
 * Establish a connection to the mongodb database
 * development database is assumed unless NODE_ENV=production
 * 
 * A promise is returned.
 *   success:  the database client and target db
 *   failure:  the error message is returned.
*/


gnomedb.connect_server = async () => {
	let port = 27017
	if (config.has("database.port")) {
		port = config.get("database.port")
	}

	let auth = false
	if (config.has("database.auth")) {
		auth = config.get("database.auth")
	}

	let targetDB = "development"
	let user = encodeURIComponent("developer")
	let pwd = encodeURIComponent("vdL42as9")
	let authMechanism = "DEFAULT"

	switch (process.env.NODE_ENV) {
	case "production":
		targetDB = "production"
		log.trace("[gnomedb] Database set to production")
		break
	case "development":
		log.trace("[gnomedb] Database set to development")
		break
	default:
		log.trace("\n*******************************************")
		log.trace("*                                         *")
		log.trace("* NODE_ENV not set.  Assuming development *")
		log.trace("*                                         *")
		log.trace("*******************************************")
		break
	}

	let url
	if (auth) {
		url = util.format("mongodb://%s:%s@localhost:%d",
			user, pwd, port)
	} else {
		url = util.format("mongodb://localhost:%d", port)
	}

	let client

	try {
		client = await MongoClient.connect(url)
		log.trace("[gnomedb] Connected correctly to server")
	} catch (err) {
		throw err
	}

	let result = { "client": client, "targetDB": targetDB }
	return result
}

/*
 * Insert a document into the database
 *   col_name:  the collection to insert the document into
 *   doc:       the json document to insert
 * 
 * A promise is returned
 *   success: the json document added to the database 
 *            including the assigned objectid
 *   failure: the error message is returned
 * 
*/
gnomedb.insertOne = async (col_name, doc) => {
	let db_con
	let result

	try {
		db_con = await gnomedb.connect_server()

		// Access production database
		let db = db_con.client.db(db_con.targetDB)

		// Select collection for insert
		let col = db.collection(col_name)

		// Insert document
		result = await col.insertOne(doc)
	} catch (err) {
		throw err
	}

	db_con.client.close()

	// return the doc for method chaining
	return result.ops[0]
}

/*
 * Insert an array of documents into the database
 *   col_name:  the collection to insert the document into
 *   docs:       array of json documents to insert
 * 
 * A promise is returned
 *   success: the array of json document added to the database 
 *            including the assigned objectid
 *   failure: the error message is returned
 * 
*/
gnomedb.insertMany = async (col_name, docs) => {
	let db_con
	let result

	try {
		db_con = await gnomedb.connect_server()

		// Access production database
		let db = db_con.client.db(db_con.targetDB)

		// Select collection for insert
		let col = db.collection(col_name)

		// Insert document
		result = await col.insertMany(docs)
	} catch (err) {
		throw err
	} finally {
		db_con.client.close()
	}

	// return the array of docs for method chaining
	return result.ops
}

/**
 * Replaces first document to match selector
 *  col_name:   	collection to search
 *  selector:   	{key : value}
 *  replacement:	document with updated fields
 * 
 * A promise is returned:
 *  success: a result object is returned with the keys:
 *    n:          The total count of documents scanned
 *    nModified:  The total count of documents modified.
 *    ok:         Is 1 if the command executed correctly.
 * 
 *  failure:  the error is returned
 */
gnomedb.replaceOne = async (col_name, selector, replacement, isUpsert = false) => {
	log.trace("[gnomedb.replaceOne] " + col_name)
	let db_con
	let result
	
	try {
		db_con = await gnomedb.connect_server()

		// Access production database
		let db = db_con.client.db(db_con.targetDB)

		// Select collection for update
		let col = db.collection(col_name)

		// update document
		result = await col.replaceOne(selector, replacement, {upsert: isUpsert})
	} catch (err) {
		throw err
	} finally {
		db_con.client.close()
	}

	return result
}

gnomedb.updateOne = async (col_name, selector, update, isUpsert = false) => {
	log.trace("[gnomedb.updateOne] " + col_name)
	let db_con
	let result
	
	try {
		db_con = await gnomedb.connect_server()

		// Access production database
		let db = db_con.client.db(db_con.targetDB)

		// Select collection for update
		let col = db.collection(col_name)

		// update document
		result = await col.updateOne(selector, update, {upsert: isUpsert})
	} catch (err) {
		throw err
	} finally {
		db_con.client.close()
	}

	return result
}

/*
 * Find all documents that satisfy a query
 *   db:       the database connection as a promise
 *   col_name: the collection to search
 *   query:    a json query.  To find someone with lastname
 *             Smith, use {'lname': 'Smith'}.  To find
 *             everyone in the collection, use {}
 * 
 *   A promise is returned:
 *     success: an array of documents which satisfy the
 *              query is returned
 *     failure: the error message is returned
*/
gnomedb.find = async (col_name, query, sort_def, limit = 0) => {
	let db_con
	let docs

	try {
		db_con = await gnomedb.connect_server()

		// Access production database
		let db = db_con.client.db(db_con.targetDB)

		// Select collection
		let col = db.collection(col_name)

		// Retrieve documents
		if (sort_def === undefined) {
			docs = await col.find(query).limit(limit).toArray()
		} else {
			docs = await col.find(query).sort(sort_def).limit(limit).toArray()
		}
	} catch (err) {
		log.error("[gnomedb.find] Database error:" + err.message)
		throw err
	} finally {
		db_con.client.close()
	}

	return docs
}

/*
 * Find all documents that satisfy a query
 *   db:       the database connection as a promise
 *   col_name: the collection to search
 *   query:    a json query.  To find someone with lastname
 *             Smith, use {'lname': 'Smith'}.  To find
 *             everyone in the collection, use {}
 * 
 *   A promise is returned:
 *     success: the document which satisfies the
 *              query is returned
 *     failure: the error message is returned
*/
gnomedb.findOne = async (col_name, query, projection = {}) => {
	let db_con
	let doc

	try {
		db_con = await gnomedb.connect_server()

		// Access production database
		let db = db_con.client.db(db_con.targetDB)

		// Select collection
		let col = db.collection(col_name)

		// Retrieve documents
		doc = await col.findOne(query, {projection: projection})
	} catch (err) {
		log.error("[gnomedb.findOne] Database error:" + err.message)
		throw err
	} finally {
		db_con.client.close()
	}

	return doc
}

/**
 * Finds First Document That Satisfies Filter And Replaces With Replacement
 * 
 * col_name		: the collection to search
 * filter		: a json query filter
 * projection	: Optional. A subset of fields to return
 * 
 * Returns the replacement document
 * 
 */
gnomedb.findOneAndReplace = async (col_name, filter, replacement, projection = {}) => {
	let db_con
	let result

	try {
		db_con = await gnomedb.connect_server()

		// Access production database
		const db = db_con.client.db(db_con.targetDB)

		// Select collection
		const col = db.collection(col_name)

		//Perform operation
		result = await col.findOneAndReplace(filter, replacement, {projection: projection, returnNewDocument: true})
	} catch (error) {
		log.error("[gnomedb.findOneAndReplace]", error)
		throw error
	} finally {
		db_con.client.close()
	}

	return result
}

/*
 * Find all documents that satisfy a query
 *   db:       the database connection as a promise
 *   col_name: the collection to search
 *   field:    the field to obtain distinct values
 * 
 *   A promise is returned:
 *     success: an array of distinct values for field
 *     failure: the error message is returned
*/
gnomedb.distinct = async (col_name, field) => {
	let db_con
	let fields

	try {
		db_con = await gnomedb.connect_server()

		// Access production database
		let db = db_con.client.db(db_con.targetDB)

		// Select collection
		let col = db.collection(col_name)

		// Retrieve documents
		fields = await col.distinct(field)
	} catch (err) {
		log.error("[gnomedb.distinct] Database error:" + err.message)
		throw err
	} finally {
		db_con.client.close()
	}

	return fields
}

gnomedb.deleteOne = async (col_name, query) => {
	let db_con
	let result

	try {
		db_con = await gnomedb.connect_server()

		// Access production database
		let db = db_con.client.db(db_con.targetDB)

		// Select collection for delete
		let col = db.collection(col_name)

		// delete document
		result = await col.deleteOne(query)
	} catch (err) {
		log.error(err, "[gnome.db.deleteOne] Database error: ")
		throw err
	} finally {
		db_con.client.close()
	}

	return result.deletedCount
}

gnomedb.findLast = async (col_name, field = "_id") => {
	let db_con
	let doc

	try {
		db_con = await gnomedb.connect_server()

		// Access production database
		let db = db_con.client.db(db_con.targetDB)

		// Select collection
		let col = db.collection(col_name)

		// Retrieve documents
		const sort = {}
		sort[field] = -1
		doc = await col.find().sort(sort).limit(1).toArray()
	} catch (err) {
		log.error("[gnomedb.findOne] Database error:" + err.message)
		throw err
	} finally {
		db_con.client.close()
	}

	return doc
}
  

module.exports = gnomedb
