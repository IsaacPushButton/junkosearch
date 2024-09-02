from junkosearch.document import Document, Field
from junkosearch.tokeniser import SimpleTokeniser, EdgeNgram
from junkosearch.writer import docs_from_csv, generate_indices


class GnafDocument(Document):
    id: str = Field(name="ADDRESS_DETAIL_PID", store=True, index=False)
    label: str = Field(name="ADDRESS_LABEL", store=True, index=True, tokenisers=[SimpleTokeniser()])


docs = docs_from_csv("./misc/GNAF_CORE.psv", GnafDocument,"|", "utf-8-sig")
generate_indices(docs)