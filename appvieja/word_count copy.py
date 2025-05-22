#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Word Counter using Apache Flink
This script implements a word counter that processes text input,
with special handling for Spanish language (including accents and special characters).
"""

import re
import sys
import os
import string
import unicodedata
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import Row, Types
from pyflink.datastream.functions import FlatMapFunction


class WordSplitter(FlatMapFunction):
    """
    Split text into words while handling Spanish text with accents and special characters.
    Performs case-insensitive processing.
    """
    def flat_map(self, line):
        # Convert to lowercase for case-insensitive counting
        line = line.lower()
        
        # Remove punctuation marks but preserve letters with accents and ñ
        # We don't use unicodedata.normalize here to keep accented characters intact
        
        # Define punctuation to remove (excluding letters with accents)
        spanish_punct = ''.join(c for c in string.punctuation)
        
        # Replace punctuation with spaces
        for char in spanish_punct:
            line = line.replace(char, ' ')
        
        # Split by whitespace and filter out empty strings
        words = [word.strip() for word in line.split() if word.strip()]
        
        # Process each word
        for word in words:
            # Additional check to ensure the word contains only valid Spanish characters
            if re.match(r'^[a-záéíóúüñ]+$', word, re.UNICODE):
                yield (word, 1)


def word_count():
    """
    Main function to set up the Flink word count application.
    Reads from a text file and counts words.
    """
    # Create the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Configure to show more detailed logging output
    env.set_parallelism(1)  # Use parallelism of 1 for local testing
    
    # Create a sample input file path
    input_file = os.path.join(os.getcwd(), "input_text.txt")
    
    # Create an empty file if it doesn't exist
    if not os.path.exists(input_file):
        with open(input_file, "w", encoding="utf-8") as f:
            f.write("Bienvenido al contador de palabras con Apache Flink\n")
            f.write("Escriba texto en español para contar palabras\n")
    
    # Read lines from the text file
    with open(input_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    # Create a data stream from the collection of lines
    text_stream = env.from_collection(lines)
    
    # Process the stream
    counts = text_stream \
        .flat_map(WordSplitter()) \
        .key_by(lambda word_count: word_count[0]) \
        .sum(1)
    
    # Print the results to the console in real-time
    counts.print()
    
    # Execute the Flink job
    env.execute("Spanish Word Count")


if __name__ == '__main__':
    print("Starting Spanish Word Count application...")
    print("Instructions:")
    print("1. Edit the 'input_text.txt' file in this directory")
    print("2. Add Spanish text to the file")
    print("3. The program will count words and display results")
    print("4. Press Ctrl+C to exit")
    
    input_file = os.path.join(os.getcwd(), "input_text.txt")
    print(f"\nInput file: {input_file}")
    
    try:
        word_count()
    except KeyboardInterrupt:
        print("\nStopping the Flink application...")
        sys.exit(0)
    except Exception as e:
        print(f"Error: {e}")
        print("\nTroubleshooting tips:")
        print("1. Make sure the input_text.txt file exists and is writable")
        print("2. Make sure your virtual environment is activated")
        print("3. Try adding some text to the input file manually")
        sys.exit(1)

