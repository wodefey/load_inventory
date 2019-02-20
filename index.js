const MongoClient = require("mongodb").MongoClient
const fs = require('fs');
const parse = require('csv-parse/lib/sync');
const util = require("util")

const data = fs.readFileSync('inventory-1_0.csv');
const rows = parse(data, {columns: true, trim: true});

const inventory = [];

for (var item = 0; item < rows.length; item++) {
    var inv_item = {};

	inv_item.sku = item + 2;
	
    inv_item.name = rows[item]['Name'];

    inv_item.primary_category = rows[item]['Primary Category'];

    let other = rows[item]['Other Categories'].slice(1,-1);
    inv_item.other_categories  = parse(other,{delimiter:'?', trim: true})[0];
	
    let flags = rows[item]['Flags'];
    inv_item.flags  = parse(flags,{delimiter:',', trim: true})[0];
	
    let tags = rows[item]['Tags'];
    inv_item.tags  = parse(tags,{delimiter:',', trim: true})[0];
	
	inv_item.bulk = (rows[item]['Bulk'] == 'TRUE');

	inv_item.grocery = (rows[item]['Grocery'] == 'TRUE');

    inv_item.manufacturer = rows[item]['manufacturer'];

    inv_item.primary_supplier = rows[item]['Primary supplier'];

    inv_item.cost = parseFloat(rows[item]['Cost']);

    inv_item.price = parseFloat(rows[item]['Price']);

    inv_item.weight = 1.0;
    inv_item.length = 1.0;
    inv_item.width = 1.0;
    inv_item.height = 1.0;
    
    inv_item.last_recieved = new Date(2019,1,23);
    inv_item.last_inventory = new Date(2019,1,23)
    inv_item.last_sold = new Date(2019,1,23)

    inv_item.inventory_low = 0;
    inv_item.inventory_par = 100;
	inv_item.inventory = 100;
	
    inventory.push(inv_item);
}

(async () => {
    let client;
  
    try {
		const port = 7854;
		const targetDB = "production";
		const user = encodeURIComponent("developer");
		const pwd = encodeURIComponent("vdL42as9");
		const authMechanism = "DEFAULT";
	
		const url = util.format("mongodb://%s:%s@localhost:%d", user, pwd, port);

		client = await MongoClient.connect(url);

		// Access production database
		const db = client.db(targetDB);
  
		// Select collection for insert
		const col = db.collection('inventory');

		// Drop existing inventory collection
		let result = await col.drop();
  
		// Insert test inventory documents
		result = await col.insertMany(inventory);
    } catch (err) {
		console.log(err.message);
    } finally {
		client.close();
    }
})();