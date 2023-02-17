import { Client } from "@opensearch-project/opensearch";
import { default as esb, script } from "elastic-builder";

const year = 2019;
const requestBody = new esb.RequestBodySearch()
	.query(
		new esb.BoolQuery()
			.filter(
				new esb.RangeQuery("first-seen")
					.gte(year)
					.lt(year + 1)
					.format("yyyy")
			)
			.filter(new esb.ExistsQuery("references"))
	)
	.size(10)
	.sort(new esb.Sort("_doc"))
	.source("references");

const client = new Client({
	node: "http://localhost:9200",
});

async function* getDocuments(client: Client, year: Number) {
	var response = await client.search({
		index: "ipfs_files",
		body: requestBody.toJSON(),
		scroll: "1m",
	});

	console.log(response.body);

	console.info(`Query returned ${response.body.hits.total.value} results.`);

	yield await response.body.hits.hits;

	// console.debug(response.body._scroll_id);

	while (response.body.hits.hits.length > 0) {
		response = await client.scroll({
			body: {
				scroll: "1m",
				scroll_id: response.body._scroll_id,
			},
		});

		yield await response.body.hits.hits;
	}
}

async function main() {
	for await (const doc of getDocuments(client, year)) {
		console.log(doc);
	}
}

main();
