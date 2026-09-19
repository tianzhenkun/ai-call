from io import BytesIO
from xml.etree import ElementTree
from zipfile import ZipFile

import pytest
from openpyxl import Workbook

from app.services.reach_email.content import parse_recipients, render_content, validate_variables


def workbook(rows):
    book = Workbook()
    for row in rows:
        book.active.append(row)
    output = BytesIO()
    book.save(output)
    return output.getvalue()


def test_import_reports_duplicates_and_excel_row_numbers():
    result = parse_recipients(
        "list.xlsx",
        workbook([
            ["客户姓名", "邮箱", "企业名称", "职务", "需求描述"],
            ["甲", "a@example.com", "公司", "", "咨询"],
            ["乙", "a@example.com", "", "", ""],
            ["丙", "broken@", "", "", ""],
        ]),
    )
    assert result["validCount"] == 1
    assert result["duplicateCount"] == 1
    assert result["errorCount"] == 1
    assert result["issues"][0]["row"] == 3
    assert result["issues"][0]["keptRow"] == 2
    assert not result["canCreate"]


def test_import_rejects_malformed_local_part_and_domain_labels():
    invalid = [".a@qq.com", "a.@qq.com", "a@-qq.com", "a@qq-.com", "a" * 65 + "@qq.com"]
    result = parse_recipients("list.xlsx", workbook([["邮箱"], *[[v] for v in invalid]]))
    assert result["errorCount"] == len(invalid)
    assert result["validCount"] == 0
    assert not result["canCreate"]


def test_import_issue_keeps_original_address_while_deduplicating_normalized_value():
    original = "  USER@qq.com的  "
    result = parse_recipients(
        "list.xlsx", workbook([["邮箱"], [original], ["user@qq.com的"], [" bad@ "]])
    )
    assert result["records"][0]["originalEmail"] == original
    assert result["records"][0]["values"]["邮箱"] == "user@qq.xn--com-5w2h"
    assert result["issues"][0]["email"] == "user@qq.com的"
    assert result["issues"][0]["keptRow"] == 2
    assert result["issues"][1]["email"] == " bad@ "


def test_variable_mapping_distinguishes_missing_column_from_empty_cell():
    validate_variables("{{需求描述}} {{职务}}", ["需求描述", "职务"])
    with pytest.raises(ValueError, match="职务"):
        validate_variables("{{职务}}", ["邮箱"])
    rendered, empty = render_content(
        "<p>{{需求描述}} {{职务}}</p>", {"需求描述": "<script>x</script>", "职务": ""}
    )
    assert "<script>" not in rendered
    assert empty == ["职务"]
    assert render_content("{{客户姓名}}", {"客户姓名": "{{职务}}"})[0] == "{{职务}}"


def test_csv_disguised_as_excel_and_formula_cells_rejected():
    with pytest.raises(ValueError):
        parse_recipients("list.xlsx", b"email,name")
    result = parse_recipients(
        "list.xlsx", workbook([["邮箱", "客户姓名"], ["a@example.com", '=HYPERLINK("bad")']])
    )
    assert result["errorCount"] == 1


def test_import_without_optional_sheet_dimensions():
    payload = workbook([["邮箱"], ["a@example.com"]])
    output = BytesIO()
    with ZipFile(BytesIO(payload)) as source, ZipFile(output, "w") as target:
        for entry in source.infolist():
            data = source.read(entry.filename)
            if entry.filename == "xl/worksheets/sheet1.xml":
                root = ElementTree.fromstring(data)
                root.remove(root.find("{*}dimension"))
                data = ElementTree.tostring(root)
            target.writestr(entry, data)
    result = parse_recipients("list.xlsx", output.getvalue())
    assert result["canCreate"]
    assert result["validCount"] == 1
    assert result["records"][0]["values"]["邮箱"] == "a@example.com"
