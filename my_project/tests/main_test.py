from my_project import main
from unittest.mock import patch, MagicMock


def test_find_all_taxis():
    taxis = main.find_all_taxis()
    assert taxis.count() > 5


def test_main_function():
    """Test that main() calls find_all_taxis and shows results"""
    with patch('my_project.main.find_all_taxis') as mock_find_all_taxis:
        mock_df = MagicMock()
        mock_find_all_taxis.return_value = mock_df
        
        main.main()
        
        mock_find_all_taxis.assert_called_once()
        mock_df.show.assert_called_once_with(5)
