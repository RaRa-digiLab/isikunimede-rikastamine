import argparse
import requests
from tqdm import tqdm
from pymarc import MARCReader, MARCWriter, Record, Field, Subfield


def viaf_request(identifier: str) -> dict:
    # Making a request to VIAF API
    viaf_response = requests.get(f'https://www.viaf.org/viaf/sourceID/ERRR|{identifier}/justlinks.json')

    # Check if the request was successful
    if viaf_response.status_code == 200:
        try:
            viaf_data = viaf_response.json()
            # Extracting ISNI, VIAF, and WKP codes
            isni_code = viaf_data.get('ISNI', [''])[0] if 'ISNI' in viaf_data else None
            viaf_code = viaf_data.get('viafID', '') if 'viafID' in viaf_data else None
            wkp_code = viaf_data.get('WKP', [''])[0] if 'WKP' in viaf_data else None
            response_codes = {"ISNI": isni_code,
                              "VIAF": viaf_code,
                              "WKP": wkp_code}

        except ValueError as e:
            print(f"Error parsing VIAF response JSON: {e}")
            return None
        return response_codes
    else:
        print(f"VIAF API request failed with status code {viaf_response.status_code}")


def update_record(record: Record, response_codes: dict) -> Record:

    # Finding the index of the last fixed field manually
    last_fixed_index = -1
    for i, field in enumerate(record.fields):
        if isinstance(field, Field) and field.is_control_field():
            last_fixed_index = i

    # Creating a new list of fields
    new_fields = []

    # Copying existing fields up to the last fixed field
    new_fields.extend(record.fields[:last_fixed_index + 1])

    # New 024 field for ISNI (if existing)
    if response_codes["ISNI"]:
        new_fields.append(Field(
            tag='024',
            indicators=['7', ' '],
            subfields=[
                Subfield('a', f'{response_codes["ISNI"]}'),
                Subfield('2', 'isni')
            ]
        ))

    # New 024 field for VIAF (if existing)
    if response_codes["VIAF"]:
        new_fields.append(Field(
            tag='024',
            indicators=['7', ' '],
            subfields=[
                Subfield('a', f'http://viaf.org/viaf/{response_codes["VIAF"]}'),
                Subfield('2', 'uri')
            ]
        ))

    # New 024 field for Wikidata (if existing)
    if response_codes["WKP"]:
        new_fields.append(Field(
            tag='024',
            indicators=['8', ' '],
            subfields=[
                Subfield('a', f'https://www.wikidata.org/wiki/{response_codes["WKP"]}')
            ]
        ))

    # Adding the remaining fields
    new_fields.extend(record.fields[last_fixed_index + 1:])
    record.fields = new_fields

    return record


def process_marc_records(input_marc_file_path, output_marc_file_path, num_records_to_process=None):
    
    # Opening the input file for reading, creating a MARCReader
    input_marc_file = open(input_marc_file_path, 'rb')
    reader = MARCReader(input_marc_file)
    print("data loaded")
    #print(len(list(reader)))

    # Creating a MARCWriter for writing the output file
    output_marc_file = open(output_marc_file_path, 'wb')
    writer = MARCWriter(output_marc_file)

    # Processing records
    if num_records_to_process:
        records_iterator = tqdm(enumerate(reader), total=num_records_to_process)
    else:
        records_iterator = enumerate(reader)

    for record_count, record in records_iterator:
        # Extracting the identifier from the record
        identifier = record['001'].value()
        # Make a VIAF request and return ISNI, VIAF, WKP codes
        viaf_response_codes = viaf_request(identifier)
        # Ipdate the existing record with codes
        updated_record = update_record(record=record, response_codes=viaf_response_codes)
        # Writing the output file
        writer.write(record)

        # Checking if a specific number of records to process is specified
        if num_records_to_process is not None and record_count + 1 >= num_records_to_process:
            break

    # Closing the MARC files
    input_marc_file.close()
    output_marc_file.close()


if __name__ == "__main__":
    # Create the parser
    parser = argparse.ArgumentParser(description='Process MARC records with VIAF requests.')

    # Add arguments to the parser
    parser.add_argument('input_path', type=str, help='Path to the input MARC file.')
    parser.add_argument('output_path', type=str, help='Path to the output MARC file.')
    parser.add_argument('--max_records', type=int, default=None, help='Maximum number of records to process (optional).')

    # Parse the arguments
    args = parser.parse_args()

    # Now, use the parsed arguments
    input_path = args.input_path
    output_path = args.output_path
    max_records = args.max_records

    # Call the function with the parsed arguments
    process_marc_records(input_path, output_path, max_records)