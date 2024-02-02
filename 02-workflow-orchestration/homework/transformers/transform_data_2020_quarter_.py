if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


def format_column_name(column_name: str):
    if 'I' in column_name:
        name_as_list = list(column_name)
        for i in range(len(name_as_list)):
            if name_as_list[i] == 'I':
                name_as_list.insert(i, '_')
                break
        column_name = "".join(name_as_list)
    
    column_name = column_name.replace(' ', '_').lower()
    return column_name



@transformer
def transform(data, *args, **kwargs):
    
    # Remove rows where the passenger count is equal to 0 or the trip distance is equal to zero.
    data = data[(data['passenger_count'] != 0) & (data['trip_distance'] != 0)]

    # Create a new column lpep_pickup_date by converting lpep_pickup_datetime to a date.
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    # Rename columns in Camel Case to Snake Case, e.g. VendorID to vendor_id.
    data.columns = [format_column_name(name) for name in data.columns]

    print(data['vendor_id'].unique())

    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert (output['passenger_count'] > 0).all(), "Some passenger counts are not greater than 0"
    assert (output['trip_distance'] > 0).all(), "Some trip distance are not greater than 0"
    assert 'vendor_id' in output.columns, 'columns not formatted'
