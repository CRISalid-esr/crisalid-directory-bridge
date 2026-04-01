import pytest
from tasks.spreadsheet.convert_spreadsheet_structures import _parse_identifier_value


class TestParseIdentifierValue:
    """Test parsing of identifier values with optional dates"""

    def test_simple_identifier_no_dates(self):
        """Test simple identifier without dates"""
        result = _parse_identifier_value("PATHO_UNIT")
        assert result == {'value': 'PATHO_UNIT'}

    def test_identifier_with_date_range(self):
        """Test identifier with start and end date"""
        result = _parse_identifier_value("E6[20190902-20251231]")
        assert result == {
            'value': 'E6',
            'start_date': '20190902',
            'end_date': '20251231'
        }

    def test_identifier_with_start_date_only(self):
        """Test identifier with start date only (open-ended)"""
        result = _parse_identifier_value("E42[20260101-]")
        assert result == {
            'value': 'E42',
            'start_date': '20260101',
            'end_date': None
        }

    def test_identifier_with_position_and_dates(self):
        """Test identifier with position and date range"""
        result = _parse_identifier_value("PATHO[1][20190902-20251231]")
        assert result == {
            'value': 'PATHO',
            'position': '1',
            'start_date': '20190902',
            'end_date': '20251231'
        }

    def test_identifier_with_position_and_open_end_date(self):
        """Test identifier with position and open-ended date"""
        result = _parse_identifier_value("PATHO[2][20260101-]")
        assert result == {
            'value': 'PATHO',
            'position': '2',
            'start_date': '20260101',
            'end_date': None
        }

    def test_identifier_with_whitespace(self):
        """Test identifier with surrounding whitespace"""
        result = _parse_identifier_value("  E6[20190902-20251231]  ")
        assert result == {
            'value': 'E6',
            'start_date': '20190902',
            'end_date': '20251231'
        }
