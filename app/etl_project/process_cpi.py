import pandas as pd


def extract_from_file(filename: str):
    df = pd.read_excel(filename)
    print(df.head())
    print(f"{len(df)} rows")

    # pass


if __name__ == "__main__":
    extract_from_file("data/Exports Merchandise, Customs, Price, US$, seas. adj..xlsx")
