import os
from collections import Counter

def read_in_chunks(file_path, chunk_size=1024 * 1024 * 1024):  # 1GB
    with open(file_path, 'r', encoding='utf-8') as file:
        while True:
            start_pos = file.tell()
            file.seek(start_pos + chunk_size, os.SEEK_SET)
            buffer = file.read(chunk_size)
            if not buffer:
                break
            last_newline = buffer.rfind('\n')
            if last_newline != -1:
                file.seek(start_pos + last_newline + 1)
                yield buffer[:last_newline + 1]
            else:
                file.seek(start_pos)
                yield file.read(chunk_size)

def main(file_path):
    word_count = Counter()
    total_size = os.path.getsize(file_path)
    processed_size = 0

    for chunk in read_in_chunks(file_path, chunk_size=1024 * 1024 * 1024):  # 1GB
        words = chunk.split()
        word_count.update(words)
        processed_size += len(chunk)
        progress = (processed_size / total_size) * 100
        print(f"Progress: {progress:.2f}%")

    with open('wordcount_result.txt', 'w') as output_file:
        for word, count in word_count.items():
            output_file.write(f"{word}: {count}\n")


if __name__ == "__main__":
    file_path = '/tools/hadoop/part-r-00000'  # Replace with your file path
    main(file_path)
