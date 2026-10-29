import logging
import sys
from multiprocessing import Process, Queue, current_process
from pathlib import Path
from threading import Thread, RLock, current_thread
from time import time

WORKERS = 4


def search(files, keywords):
    """Повертає {слово: [шляхи файлів, де воно є]}."""
    result = {k: [] for k in keywords}
    for file in files:
        try:
            text = file.read_text(encoding="utf-8", errors="ignore").lower()
        except OSError as e:
            logging.error(f"Не вдалося прочитати {file}: {e}")
            continue
        for k in keywords:
            if k.lower() in text:
                result[k].append(str(file))
    return result


def merge(total, part):
    for k, paths in part.items():
        total[k].extend(paths)


def thread_worker(files, keywords, total, lock):
    logging.debug(f"{current_thread().name} обробляє {len(files)} файл(ів)")
    part = search(files, keywords)
    with lock:
        merge(total, part)


def threading_search(files, keywords):
    total = {k: [] for k in keywords}
    lock = RLock()
    threads = [
        Thread(target=thread_worker, args=(files[i::WORKERS], keywords, total, lock))
        for i in range(WORKERS)
    ]
    [t.start() for t in threads]
    [t.join() for t in threads]
    return total


def process_worker(files, keywords, queue):
    logging.debug(f"{current_process().name} обробляє {len(files)} файл(ів)")
    queue.put(search(files, keywords))


def multiprocessing_search(files, keywords):
    total = {k: [] for k in keywords}
    queue = Queue()
    processes = [
        Process(target=process_worker, args=(files[i::WORKERS], keywords, queue))
        for i in range(WORKERS)
    ]
    [p.start() for p in processes]
    for _ in processes:
        merge(total, queue.get())  # читаємо чергу до join, щоб процеси не зависли
    [p.join() for p in processes]
    return total


def get_files(directory):
    path = Path(directory)
    if not path.is_dir():
        sys.exit(f"Папка не знайдена: {directory}")
    files = list(path.glob("*.txt"))
    if not files:
        sys.exit(f"У папці {directory} немає .txt файлів")
    return files


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format="%(message)s", stream=sys.stdout)

    # python main.py [папка] [слово1 слово2 ...]
    directory = sys.argv[1] if len(sys.argv) > 1 else "files"
    keywords = sys.argv[2:] or ["heart", "never", "time", "dream"]
    files = get_files(directory)
    print(f"Файлів: {len(files)}, слова: {keywords}, workers: {WORKERS}")

    for name, func in [("Threading", threading_search), ("Multiprocessing", multiprocessing_search)]:
        print(f"\n--- {name} ---")
        start = time()
        result = func(files, keywords)
        print(f"Час: {time() - start:.4f} c")
        for k, paths in result.items():
            print(f"  {k}: {sorted(paths)}")
