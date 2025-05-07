from graphology import Fetcher, Processor


def main():
    fetcher = Fetcher(start_year=2020, stop_year=2021)
    timestamp: str = fetcher.fetch()

    processor = Processor(timestamp=timestamp)
    processor.process()
    processor.merge()


if __name__ == "__main__":
    main()
