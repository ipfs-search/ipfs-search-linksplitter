import { default as crypto } from "crypto";

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

	console.info(`Query returned ${response.body.hits.total.value} results.`);

	for (const hit of response.body.hits.hits) {
		yield hit;
	}

	while (response.body.hits.hits.length > 0) {
		response = await client.scroll({
			body: {
				scroll: "1m",
				scroll_id: response.body._scroll_id,
			},
		});

		for (const hit of response.body.hits.hits) {
			yield hit;
		}
	}
}

function base64RemovePadding(str: string): string {
	return str.replace(/={1,2}$/, "");
}

function hashId(input: string): string {
	return base64RemovePadding(
		crypto.createHash("sha1").update(input).digest("base64")
	);
}

async function* getLinks(documents: AsyncGenerator) {
	for await (const doc of getDocuments(client, year)) {
		const to = doc._id;

		for (const ref of doc._source.references) {
			const from = ref.parent_hash,
				name = ref.name;

			yield {
				_id: hashId(`ipfs://${from}-ipfs://${to}-${name}`),
				_source: {
					from: from,
					to: to,
					name: name,
				},
			};
		}
	}
}

async function main() {
	const docs = getDocuments(client, year);
	for await (const link of getLinks(docs)) {
		console.log(link);
	}
}

main();
