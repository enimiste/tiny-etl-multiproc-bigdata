import os
import re
import argparse
import logging
import re
import time

import apache_beam as beam
from apache_beam.transforms import PTransform
from apache_beam.io import ReadAllFromText, ReadFromTextWithFilename
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from commons import truncate_str_270, truncate_str_255

from commons import basename_backwards_x4, extract_files_from_dir, format_duree

def arabic_tokenize_text(text: str, mapper):
    arabic_words = re.findall(r'[َُِْـًٌٍّؤائءآىإأبتثجحخدذرزسشصضطظعغفقكلمنهـوي]+', text)
    res_words = []
    for txt in arabic_words:
        words = txt.replace('×', '').replace(' ', '\n').replace('\r', '\n').replace('\t', '\n').split('\n')
        for w in words:
            if w and w.strip():
                w = mapper(w)
                if w is not None:
                    res_words.append(w)

    return res_words

def arabic_remove_diac(text: str):
    if text is None:
        return text
    return text.replace('َ', '').replace('ّ', '').replace('ِ', '').replace('ُ', '').replace('ْ', '').replace('ً', '').replace('ٌ', '').replace('ٍ', '')

def extract_arabic_words_with_filename(line_with_filename):
    (file_name, line) = line_with_filename
    # return [(truncate_str_270(basename_backwards_x4(file_name)), truncate_str_255(word)) for word in arabic_tokenize_text(line, arabic_remove_diac)]
    return [("____", truncate_str_255(word)) for word in line.split(" ")]

def extract_words_with_filename(line_with_filename):
    (file_name, line) = line_with_filename
    # return [(truncate_str_270(basename_backwards_x4(file_name)), truncate_str_255(word)) for word in line.split(" ")]
    return [("____", truncate_str_255(word)) for word in line.split(" ")]

def main(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input', 
        required=True,
        help='Input file to process.')
    parser.add_argument(
        '--ext',
        dest='ext', 
        required=True,
        help='Input file ext to process.')
    parser.add_argument(
        '--output',
        dest='output',
        required=True,
        help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_args.extend([
        # '--runner=DirectRunner',
        "--runner=FlinkRunner",
        "--flink_master=localhost:8081",
        "--environment_type=LOOPBACK",
        '--parallelism=7',
        '--job_name=arabic-words-extraction',
    ])

    known_args.output = os.path.abspath(known_args.output)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    with beam.Pipeline(options=pipeline_options) as p:

        logging.log(logging.INFO, 'Finding files ....')
        files = []
        if os.path.isfile(known_args.input):
            files = [known_args.input]
        elif os.path.isdir(known_args.input):
            files = extract_files_from_dir(os.path.abspath(known_args.input), known_args.ext, "file:///")
            print(files)
        else:
            raise RuntimeError('Invalid path ' + known_args.input)
        logging.log(logging.INFO, '{} files found'.format(len(files)))

        # Format the counts into a PCollection of strings.
        def format_result(word_count):
            (x, count) = word_count
            (file_name, word) = x
            return '%s; %s; %s' % (word, count, file_name)

        # Count the occurrences of each word.
        words = (p 
            | 'Extract all files' >> beam.Create(files, reshuffle=False)
            | 'Read lines' >> ReadAllFromText()
            | 'Split' >> (
                # beam.FlatMap(extract_arabic_words_with_filename))
                beam.FlatMap(extract_words_with_filename))
            | 'PairWithOne' >> beam.Map(lambda  word: ((word[0], word[1]), 1))
            | 'GroupAndSum' >> beam.CombinePerKey(sum)
            | 'Format' >> beam.Map(format_result))


        # Write the output using a "Write" transform that has side effects.
        # pylint: disable=expression-not-assigned
        words | WriteToText('{}/{}_{}'.format(os.path.dirname(known_args.output), int(time.time()), os.path.basename(known_args.output)))


if __name__ == "__main__":
    """"
    ./start-cluster.sh

    python37.exe main.py --input=./input --ext=.txt --output=./output/words.csv

    ./stop-cluster.sh
    """
    logging.getLogger().setLevel(logging.INFO)
    start_exec_time = time.perf_counter()
    logging.log(logging.INFO, "Script started ({})".format(start_exec_time))
    main()
    end_exec_time=time.perf_counter()
    duree_exec = round(end_exec_time-start_exec_time, 3)
    logging.log(logging.INFO, 'Script End executed in {} sec.'.format(format_duree(duree_exec)))