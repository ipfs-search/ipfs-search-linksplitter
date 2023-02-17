import { CID } from "multiformats/cid";
import { default as crypto } from "crypto";

import { Client } from "@opensearch-project/opensearch";
import { default as esb } from "elastic-builder";

const scrollTime = "1m";
const srcIndex = "ipfs_files";
const dstIndex = "ipfs_links";
const batchSize = 10;
const year = 2019;

async function* getHits(client: Client, year: number) {
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
		.size(batchSize)
		.sort(new esb.Sort("_doc"))
		.source(["references", "last-seen"]);

	var response = await client.search({
		index: srcIndex,
		body: requestBody.toJSON(),
		scroll: scrollTime,
	});

	console.info(`Query returned ${response.body.hits.total.value} results.`);

	for (const hit of response.body.hits.hits) {
		yield hit;
	}

	// while (response.body.hits.hits.length > 0) {
	// 	response = await client.scroll({
	// 		body: {
	// 			scroll: scrollTime,
	// 			scroll_id: response.body._scroll_id,
	// 		},
	// 	});

	// 	for (const hit of response.body.hits.hits) {
	// 		yield hit;
	// 	}
	// }
}

function base64RemovePadding(str: string): string {
	return str.replace(/={1,2}$/, "");
}

function hashId(input: string): string {
	return base64RemovePadding(
		crypto.createHash("sha1").update(input).digest("base64")
	);
}

function homogeniseCID(cid: string): string {
	return CID.parse(cid).toV1().toString();
}

async function* getLinks(documents: AsyncGenerator<any, void, unknown>) {
	for await (const doc of documents) {
		const to = homogeniseCID(doc._id);
		const last_seen = doc._source["last-seen"];

		for (const ref of doc._source.references) {
			yield {
				from: homogeniseCID(ref.parent_hash),
				to: to,
				name: ref.name,
				seen: last_seen,
			};
		}
	}
}

async function main() {
	const client = new Client({
		node: "http://localhost:9200",
	});

	const docs = getHits(client, year);

	const result = await client.helpers.bulk({
		datasource: getLinks(docs),
		require_alias: true,
		onDocument(doc) {
			return {
				index: {
					_index: dstIndex,
					_id: hashId(`ipfs://${doc.from}-ipfs://${doc.to}-${doc.name}`),
				},
			};
		},
		onDrop(doc) {
			console.error("Error indexing", doc);
		},
	});
	console.log(result);
}

main();
