import { default as esb } from "elastic-builder";

const year = 2021;
const requestBody = new esb.RequestBodySearch()
	.query(
		new esb.BoolQuery()
			.filter(
				new esb.RangeQuery("first-seen")
					.gte(year)
					.lt(year + 1)
					.format("yyyy")
			)
			.should(new esb.WildcardQuery("metadata.Content-Type", "audio/*"))
			.should(new esb.TermQuery("metadata.Content-Type", "application/ogg"))
	)
	.sort(new esb.Sort("_doc"))
	.trackScores(false);

console.log(JSON.stringify(requestBody.toJSON(), null, 2));
