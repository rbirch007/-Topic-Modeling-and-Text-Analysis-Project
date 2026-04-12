#!/usr/bin/env python3
"""Debug the final 4 missing poems."""
import os, re, fitz

PDF_BASE = r"C:\Users\birch\OneDrive - George Mason University - O365 Production\Dissertation\Womenprintculture\RSmagazine"

cases = [
    (31, "I-30", "Sartori", 480, 559),  # July
    (37, "Promontory", "Cook", 0, None),  # August - full scan
    (37, "Chalk Cliff", "Harwood", 0, None),  # August - full scan
    (40, "Woman-Power", "Alleman", 0, None),  # November - full scan
]

for vol, title, author_last, start, end in cases:
    pdf_path = os.path.join(PDF_BASE, f"reliefsocietymagvol{vol}.pdf")
    doc = fitz.open(pdf_path)
    if end is None:
        end = len(doc) - 1

    print(f"\n{'='*60}")
    print(f"Vol{vol}: '{title}' by '{author_last}' (scanning {start}-{end})")

    # Try to find with various strategies
    title_lower = title.lower()
    title_nohyphen = title.replace('-', '').lower()

    found = False
    for pg in range(start, min(end + 1, len(doc))):
        text = doc[pg].get_text()
        tl = text.lower()
        tn = re.sub(r'\s+', '', tl)  # all whitespace removed

        # Check title variants
        if title_lower in tl or title_nohyphen in tl:
            print(f"  FOUND on page {pg}!")
            idx = max(tl.find(title_lower), tl.find(title_nohyphen))
            print(f"  Context: {text[max(0,idx-50):idx+200]}")
            found = True
            break

        # Check collapsed text
        title_collapsed = title_lower.replace(' ', '').replace('-', '')
        if title_collapsed in tn:
            print(f"  FOUND (collapsed) on page {pg}!")
            found = True
            break

        # Check for spaced-out version
        if len(title) > 3:
            # Make regex: P\s*r\s*o\s*m...
            chars = list(title_lower.replace('-', ''))
            pat = r'\s*'.join(re.escape(c) for c in chars)
            if re.search(pat, text, re.IGNORECASE):
                # Also verify author
                author_pat = r'\s*'.join(re.escape(c) for c in author_last.lower())
                if re.search(author_pat, text, re.IGNORECASE):
                    print(f"  FOUND (spaced) on page {pg}!")
                    # Show context
                    m = re.search(pat, text, re.IGNORECASE)
                    if m:
                        print(f"  Context: {text[max(0,m.start()-50):m.end()+200]}")
                    found = True
                    break

    if not found:
        print("  NOT FOUND anywhere in PDF")
        # Check if author exists anywhere
        for pg in range(start, min(end + 1, len(doc))):
            text = doc[pg].get_text()
            author_pat = r'\s*'.join(re.escape(c) for c in author_last.lower())
            if re.search(author_pat, text, re.IGNORECASE):
                print(f"  Author '{author_last}' found on page {pg}")
                lines = text.split('\n')
                for i, l in enumerate(lines):
                    if re.search(author_pat, l, re.IGNORECASE):
                        cs = max(0, i-2)
                        ce = min(len(lines), i+3)
                        for j in range(cs, ce):
                            print(f"    line {j}: {lines[j][:120]}")
                        break
                break

    doc.close()
