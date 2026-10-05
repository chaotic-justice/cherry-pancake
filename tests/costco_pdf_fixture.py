def make_costco_pdf(
    rows: list[list[str]],
    metadata_lines: tuple[str, ...] = (
        "Date: 09/24/2026",
        "Payment: 456",
    ),
) -> bytes:
    """Build a small table-based PDF without mocking pdfplumber internals."""
    columns = [
        "invoiceNumber",
        "orderNumber",
        "description",
        "date",
        "gross",
        "discount",
        "amount",
    ]
    table = [columns, *rows]
    x_positions = [30, 110, 235, 300, 375, 445, 500, 582]
    table_top = 700
    row_height = 24
    operations = ["BT /F1 12 Tf 40 760 Td"]
    for index, line in enumerate(metadata_lines):
        operations.append(
            f"0 {-16 if index else 0} Td ({_escape_pdf_text(line)}) Tj"
        )
    operations.extend(["ET", "0.5 w"])

    for index in range(len(table) + 1):
        y_position = table_top - index * row_height
        operations.append(
            f"{x_positions[0]} {y_position} m "
            f"{x_positions[-1]} {y_position} l S"
        )
    for x_position in x_positions:
        operations.append(
            f"{x_position} {table_top} m "
            f"{x_position} {table_top - len(table) * row_height} l S"
        )
    for row_index, row in enumerate(table):
        y_position = table_top - row_index * row_height - 16
        for column_index, value in enumerate(row):
            operations.append(
                f"BT /F1 6 Tf {x_positions[column_index] + 2} {y_position} Td "
                f"({_escape_pdf_text(value)}) Tj ET"
            )

    stream = "\n".join(operations).encode("ascii")
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        (
            b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
            b"/Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>"
        ),
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
        (
            b"<< /Length "
            + str(len(stream)).encode("ascii")
            + b" >>\nstream\n"
            + stream
            + b"\nendstream"
        ),
    ]
    pdf = bytearray(b"%PDF-1.4\n")
    offsets = [0]
    for number, obj in enumerate(objects, 1):
        offsets.append(len(pdf))
        pdf.extend(f"{number} 0 obj\n".encode("ascii"))
        pdf.extend(obj)
        pdf.extend(b"\nendobj\n")

    xref_offset = len(pdf)
    pdf.extend(f"xref\n0 {len(objects) + 1}\n".encode("ascii"))
    pdf.extend(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        pdf.extend(f"{offset:010d} 00000 n \n".encode("ascii"))
    pdf.extend(
        (
            f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\n"
            f"startxref\n{xref_offset}\n%%EOF\n"
        ).encode("ascii")
    )
    return bytes(pdf)


def _escape_pdf_text(value: str) -> str:
    return value.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")
