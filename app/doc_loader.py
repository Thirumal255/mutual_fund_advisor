import fitz

def extract_paragraphs(pdf_path):
    doc = fitz.open(pdf_path)
    items = []
    for idx, page in enumerate(doc):
        text = page.get_text()
        raw = text.replace("\r", "\n")

        # normalize multi-newline
        parts = [x.strip() for x in raw.split("\n\n") if x.strip()]
        for p in parts:
            items.append({
                "page": idx + 1,
                "text": p
            })
    doc.close()
    return items
