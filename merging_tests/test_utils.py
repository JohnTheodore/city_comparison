from merging_code.utils import remove_substring_from_end_of_string


def test_remove_substring_from_end_of_string():
  input_string = 'foo bar baz'
  new_string = remove_substring_from_end_of_string(input_string, [' baz'])
  assert new_string == 'foo bar'
  new_string2 = remove_substring_from_end_of_string(input_string,
                                                    [' baz', ' bar'])
  assert new_string2 == 'foo'
