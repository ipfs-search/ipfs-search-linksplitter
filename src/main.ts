import { CID } from "multiformats/cid";
import { default as crypto } from "crypto";

import { Client } from "@opensearch-project/opensearch";
import { default as esb } from "elastic-builder";

const scrollTime = "1m";
const srcIndex = "ipfs_files";
const dstIndex = "ipfs_links";
const batchSize = 100;
const years = [2018, 2020, 2021, 2022, 2023];
const months = [...Array(12).keys()].map((i) => i + 1);

function getEnd(year: number, month: number): string {
	if (month == 12) {
		return `${year + 1}-1`;
	}

	return `${year}-${month + 1}`;
}

function getRequestBody(year: number, month: number): esb.RequestBodySearch {
	const start = `${year}-${month}`;
	const end = getEnd(year, month);

	return new esb.RequestBodySearch()
		.query(
			new esb.BoolQuery()
				.filter(
					new esb.RangeQuery("first-seen").gte(start).lt(end).format("yyyy-M")
				)
				.filter(new esb.ExistsQuery("references"))
		)
		.size(batchSize)
		.sort(new esb.Sort("_doc"))
		.source(["references", "last-seen"]);
}

async function* getHits(client: Client, year: number, month: number) {
	const requestBody = getRequestBody(year, month);
	const search = client.helpers.scrollSearch({
		index: srcIndex,
		body: requestBody.toJSON(),
		scroll: scrollTime,
	});

	var first = true;
	for await (const result of search) {
		if (first) {
			console.info(
				`Query returned ${result.body["hits"].total.value} results.`
			);
			first = false;
		}

		for (const hit of result.body["hits"].hits) {
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

function homogeniseCID(cid: string): string {
	return CID.parse(cid).toV1().toString();
}

function removeMillis(input: string): string {
	return input.substring(0, 19) + "Z";
}

function getSeen(doc: any): string | null {
	const fields = ["last-seen", "first-seen"];

	for (const f of fields) {
		if (doc._source[f]) return removeMillis(doc._source[f]);
	}

	return null;
}

async function* getLinks(documents: AsyncIterable<any>) {
	var docCnt = 0;

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
		}

		docCnt++;
		if (docCnt % 1000 === 0) {
			console.log(`${docCnt} documents processed`);
		}
	}
}

async function processYearMonth(client: Client, year: number, month: number) {
	const docs = getHits(client, year, month);

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
		for (const month of months) {
			console.log(`Processing month ${year}-${month}`);
			const result = await processYearMonth(client, year, month);
			console.log(`Processed ${year}-${month}, result:`, result);
		}
	}
}

main();
