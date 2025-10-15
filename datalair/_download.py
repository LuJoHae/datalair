import os
from ftplib import FTP
from pathlib import Path

import requests
from tqdm import tqdm


def download_file(url: str, filepath: Path) -> None:
    # Send GET request with streaming
    response = requests.get(url, stream=True)
    total_size = int(response.headers.get("content-length", 0))
    block_size = 1024  # Size of chunks to download

    # Progress bar
    with (
        open(str(filepath), "wb") as file,
        tqdm(
            desc=str(filepath),
            total=total_size,
            unit="iB",
            unit_scale=True,
            unit_divisor=1024,
        ) as bar,
    ):
        for data in response.iter_content(chunk_size=block_size):
            file.write(data)
            bar.update(len(data))


def download_supplementary_from_geo(gse_id: str, local_dir: Path) -> None:
    ftp_host = "ftp.ncbi.nlm.nih.gov"
    ftp_dir = "/geo/series/{}nnn/{}/suppl/".format(gse_id[:-3], gse_id)

    # Connect to FTP
    ftp: FTP = FTP(ftp_host)
    ftp.login()
    ftp.cwd(ftp_dir)

    files: list[str] = ftp.nlst()

    os.makedirs(local_dir, exist_ok=True)
    for filename in files:
        local_filepath: Path = local_dir.joinpath(filename)
        with open(local_filepath, "wb") as f:
            ftp.retrbinary(f"RETR {filename}", f.write)
            print(f"Downloaded: {filename}")

    ftp.quit()


def download_files_from_arrayexpress(arrayexpress_id: str, local_dir: Path) -> None:
    ftp_host = "ftp.ebi.ac.uk"
    ftp_dir = "/biostudies/fire/E-MTAB-/{}/{}/Files".format(arrayexpress_id[-3:], arrayexpress_id)
    ftp = FTP(ftp_host)
    ftp.login()
    ftp.cwd(ftp_dir)
    files = ftp.nlst()

    os.makedirs(local_dir, exist_ok=True)
    for filename in files:
        local_filepath = local_dir.joinpath(filename)
        with open(local_filepath, "wb") as f:
            ftp.retrbinary(f"RETR {filename}", f.write)
            print(f"Downloaded: {filename}")

    ftp.quit()
