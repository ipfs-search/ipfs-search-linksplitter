import { CID } from "multiformats/cid";
import { default as crypto } from "crypto";

import { Client } from "@opensearch-project/opensearch";
import { default as esb } from "elastic-builder";

const scrollTime = "1m";
const srcIndex = "ipfs_files";
const dstIndex = "ipfs_links";
const batchSize = 100;
const years = [2016, 2017, 2018, 2020, 2021, 2022, 2023];

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

	while (response.body.hits.hits.length > 0) {
		response = await client.scroll({
			body: {
				scroll: scrollTime,
				scroll_id: response.body._scroll_id,
			},
		});

		for (const hit of response.body.hits.hits) {
			yield hit;
		}
	}

	// Delete scroll
	await client.clear_scroll({
		body: {
			scroll_id: response.body._scroll_id,
		},
	});
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

function removeMillis(input: string): string {
	return input.substring(0, 19) + "Z";
}

function getSeen(doc): string {
	const fields = ["last-seen", "first-seen"];

	for (const f of fields) {
		if (doc._source[f]) return removeMillis(doc._source[f]);
	}

	return null;
}

async function* getLinks(documents: AsyncGenerator<any, void, unknown>) {
	var docCnt = 0,
		refCnt = 0;

	for await (const doc of documents) {
		const to = homogeniseCID(doc._id);
		const seen = getSeen(doc);

		for (const ref of doc._source.references) {
			yield {
				from: homogeniseCID(ref.parent_hash),
				to: to,
				name: ref.name,
				seen: seen,
			};

			refCnt++;
			if (refCnt % 1000 === 0) {
				console.log(`${refCnt} references processed`);
			}
		}

		docCnt++;
		if (docCnt % 1000 === 0) {
			console.log(`${docCnt} documents processed`);
		}
	}
}

async function processYear(client: Client, year: number) {
	const docs = getHits(client, year);

	return client.helpers.bulk({
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
}

async function main() {
	const client = new Client({
		node: "http://localhost:9200",
	});

	for (const year of years) {
		console.log(`Processing year ${year}`);
		const result = await processYear(client, year);
		console.log(`Processed year ${year}, result:`, result);
	}
}

main();
