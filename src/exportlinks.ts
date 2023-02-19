import { Client } from "@opensearch-project/opensearch";
import { default as esb } from "elastic-builder";
import { stdout } from "node:process";
import { pipeline } from "node:stream/promises";

const batchSize = 1000;
const scrollTime = "1m";
const index = "ipfs_links";
const maxElems = 1000;

const client = new Client({
	node: "http://localhost:9200",
});

const requestBody = new esb.RequestBodySearch()
	.query(new esb.BoolQuery())
	.size(batchSize)
	.sort(new esb.Sort("_doc"))
	.source(["from", "to"]);

const scroll = client.helpers.scrollDocuments({
	index: index,
	body: requestBody,
	scroll: scrollTime,
});

async function* docToPairwise(docs) {
	var cnt = 0;
	for await (const doc of docs) {
		yield `${doc.from} ${doc.to}\n`;
		cnt++;

		if (cnt > maxElems) return;
	}
}

async function main() {
	await pipeline(scroll, docToPairwise, stdout);
}

main();
