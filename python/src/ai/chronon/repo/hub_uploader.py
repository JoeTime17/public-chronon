import glob
import hashlib
import json
import os

from ai.chronon.cli.formatter import Format, format_print
from ai.chronon.cli.theme import print_info, print_step, print_success
from ai.chronon.repo import (
    FOLDER_NAME_TO_CLASS,
    FOLDER_NAME_TO_CONF_TYPE,
)
from ai.chronon.repo.zipline_hub import ZiplineHub
from gen_thrift.api.ttypes import Conf


def build_local_repo_hashmap(root_dir: str, env: str = 'prod'):
    # `env` picks the compile output folder. Mirrors the per-env layout produced
    # by `zipline compile`: 'prod' → compiled/, any other <env> → compiled_<env>/
    # (matching teams.<env>.py). Empty / falsy env defaults to prod.
    e = (env or "prod").lower()
    folder = "compiled" if e == "prod" else f"compiled_{e}"
    compiled_dir = os.path.join(root_dir, folder)
    # Returns a map of name -> (tbinary, file_hash)
    results = {}

    # Iterate through each object type folder (staging_queries, group_bys, joins etc)
    for folder_name, _ in FOLDER_NAME_TO_CLASS.items():
        folder_path = os.path.join(compiled_dir, folder_name)
        if not os.path.exists(folder_path):
            continue

        # Find all json files recursively in this folder
        json_files = [
            f
            for f in glob.glob(os.path.join(folder_path, "**/*"), recursive=True)
            if os.path.isfile(f)
        ]

        exceptions = []

        for json_file in json_files:
            try:
                # Read the json file
                with open(json_file, "r") as f:
                    thrift_json = f.read()

                # Extract name from metadata in json
                json_obj = json.loads(thrift_json)
                name = json_obj["metaData"]["name"]

                # Load the json into the appropriate object type based on folder
                # binary = json2binary(thrift_json, obj_class)

                md5_hash = hashlib.md5(thrift_json.encode()).hexdigest()
                # md5_hash = hashlib.md5(thrift_json.encode()).hexdigest() + "123"
                # results[name] = (binary, md5_hash, FOLDER_NAME_TO_CONF_TYPE[folder_name])
                results[name] = Conf(
                    name=name,
                    hash=md5_hash,
                    # contents=binary,
                    contents=thrift_json,
                    confType=FOLDER_NAME_TO_CONF_TYPE[folder_name],
                    localPath=json_file
                )

            except Exception as e:
                exceptions.append(f"{json_file} - {e}")

        if exceptions:
            error_msg = (
                "The following files had exceptions during upload: \n"
                + "\n".join(exceptions)
                + "\n\n Consider deleting the files (safe operation) and checking "
                + "your thrift version before rerunning your command."
            )
            raise RuntimeError(error_msg)

    return results


def compute_and_upload_diffs(
        branch: str,
        zipline_hub: ZiplineHub,
        local_repo_confs: dict[str, Conf],
        format: Format = Format.TEXT,
) -> dict[str, Conf]:
    # Determine which confs are different from the ZiplineHub
    # Call Zipline hub with `names_and_hashes` as the argument to get back
    names_to_hashes = {name: local_conf.hash for name, local_conf in local_repo_confs.items()}
    print_step(f"🧮 Computed hashes for {len(names_to_hashes)} local files.", format=format)

    changed_conf_names: list[str] = zipline_hub.call_diff_api(names_to_hashes)["diff"]

    if not changed_conf_names:
        print_success(
            f"Remote contains all local files. No need to upload '{branch}'.", format=format
        )
        diffed_confs = {}
    else:
        unchanged = len(names_to_hashes) - len(changed_conf_names)
        print_info(
            f"🔍 Detected {len(changed_conf_names)} changes on local branch '{branch}'. {unchanged} unchanged.",
            format=format,
        )

        # a list of names for diffed hashes on branch
        diffed_confs = {k: local_repo_confs[k] for k in changed_conf_names}

        conf_names_str = "\n    - ".join(diffed_confs.keys())
        format_print(f"    - {conf_names_str}", format=format)

        diff_confs = []
        for _, conf in diffed_confs.items():
            diff_confs.append(conf.__dict__)

        # Make PUT request to ZiplineHub
        zipline_hub.call_upload_api(branch=branch, diff_confs=diff_confs)
        print_step(
            f"⬆️ Uploaded {len(diffed_confs)} changed confs to branch '{branch}'.", format=format
        )

    zipline_hub.call_sync_api(branch=branch, names_to_hashes=names_to_hashes)

    print_success(f"{len(names_to_hashes)} hashes updated on branch '{branch}'.", format=format)
    return diffed_confs
