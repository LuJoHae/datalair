"""
This module provides functionality to download files from various sources, such as public
databases like GEO, ArrayExpress, or any generic URL. It supports FTP and HTTP(S) protocols and
includes progress tracking during downloads.

Functions:
    download_file(url: str, filepath: Path) -> None:
        Downloads a file from the specified URL and saves it to the given file path.

    download_supplementary_from_geo(gse_id: str, local_dir: Path) -> None:
        Downloads supplementary files for a given GSE ID from the GEO database via FTP.

    download_files_from_arrayexpress(arrayexpress_id: str, local_dir: Path) -> None:
        Downloads files from an ArrayExpress dataset via FTP.

Dependencies:
    - ftplib: For handling FTP-based downloads.
    - pathlib: For handling file paths in a platform-independent manner.
    - requests: For handling HTTP(S) file downloads.
    - tqdm: For visually tracking download progress.

"""

import os
from ftplib import FTP
from pathlib import Path

import requests
from tqdm import tqdm


def download_file(url: str, filepath: Path) -> None:
    """Downloads a file from the given URL and saves it to the specified file path.

    This function sends a GET request to the provided URL and streams the content in chunks
    to avoid overwhelming the memory. It displays the download progress using the tqdm
    library and writes the data to the specified file path.

    Args:
        url (str): The URL of the file to be downloaded.
        filepath (Path): The path of the file where the downloaded content will be saved.

    """
    # Send GET request with streaming
    response = requests.get(url, stream=True)
    total_size = int(response.headers.get("content-length", 0))
    block_size = 1024  # Size of chunks to download

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
    """
    Downloads supplementary files from the GEO (Gene Expression Omnibus) database for a given GSE
    ID and saves them to a specified local directory.

    The function connects to the National Center for Biotechnology Information (NCBI) FTP server,
    navigates to the directory containing supplementary files for the specified GSE ID, and
    downloads each file to the local directory. If the directory does not exist, it will be
    created.

    Args:
        gse_id: Gene Series Expression (GSE) ID used to locate the supplementary files in the GEO database.
        local_dir: Local directory where the downloaded files will be stored.

    """
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
    """
    Downloads files from an ArrayExpress dataset to a local directory.

    This function connects to the ArrayExpress FTP server, navigates to the
    specific dataset directory using the given `arrayexpress_id`, and downloads
    all the files into the specified local directory.

    Args:
        arrayexpress_id (str): The unique identifier of the ArrayExpress dataset.
        local_dir (Path): The local directory where the files will be downloaded.
    """
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
