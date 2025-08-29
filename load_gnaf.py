from junkosearch.document import Document, Field
from junkosearch.tokeniser import SimpleTokeniser, EdgeNgram
from junkosearch.writer import docs_from_csv, generate_indices


class GnafDocument(Document):
    id: str = Field(source_name="ADDRESS_DETAIL_PID", store=True, index=False)
    label: str = Field(source_name="ADDRESS_LABEL", store=True, index=True, query_code="label_word_ngram", tokenisers=[EdgeNgram(min_len=3, max_len=10, split=" ")])
    label_full: str = Field(source_name="ADDRESS_LABEL", store=False, index=True, query_code="label_sent_ngram", tokenisers=[EdgeNgram(min_len=6, max_len=20, split=None)])
    locality_name: str = Field(source_name="LOCALITY_NAME", store=False, index=True, query_code="locname", tokenisers=[SimpleTokeniser()])
    street_name: str = Field(source_name="STREET_NAME", store=False, index=True, query_code="streetname", tokenisers=[SimpleTokeniser()])
    street_number_1: str = Field(source_name="NUMBER_FIRST", store=False, index=True, query_code="snumber1", tokenisers=[SimpleTokeniser()])

if __name__ == "__main__":
    docs = docs_from_csv("./misc/GNAF_CORE.psv", GnafDocument,"|", "utf-8-sig")
    generate_indices(docs, seg_size=1024*1024*600)