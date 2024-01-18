import collections
import os

def read_in_chunks(file_path, chunk_size=4096):
    """
    Generator to read a file in chunks.
    """
    with open(file_path, 'r', encoding='utf-8') as file:
        while True:
            start_pos = file.tell()
            file.seek(os.SEEK_CUR, chunk_size)
            buffer = file.read()
            if not buffer:
                break
            last_newline = buffer.rfind('\n')
            if last_newline != -1:
                file.seek(start_pos + last_newline + 1)
                yield buffer[:last_newline + 1]
            else:
                # Handle case where there are no newlines in the buffer
                file.seek(start_pos)
                yield file.read(chunk_size)

def count_words_in_chunk(chunk):
    """
    Count words in a given chunk of text.
    """
    words = chunk.split()
    word_counts = collections.Counter(words)
    return word_counts

def main(file_path):
    word_count = collections.Counter()
    for chunk in read_in_chunks(file_path, chunk_size=4 * 1024 * 1024 * 1024):
        word_count += count_words_in_chunk(chunk)

    for word, count in word_count.items():
        print(f"{word}: {count}")

if __name__ == "__main__":
    file_path = '/path/to/your/largefile.txt'  # Replace with your file path
    main(file_path)
