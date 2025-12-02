import argparse
import hashlib
import json
import os
import sqlite3
import sys
import tarfile
import tempfile
from pathlib import Path
from typing import Optional

from Crypto.Cipher import AES


def connect_to_database(path: Path):
    """Connects to the DHT SQLite database. Expects an existing DHT database."""
    conn = sqlite3.connect(path)
    cur = conn.cursor()

    res = cur.execute("SELECT value FROM metadata WHERE key = 'version'")

    if res.fetchone():
        return conn
    else:
        raise ValueError("Database is not initialized.")


def extract_zalo_backup(dkey: bytes, path: Path) -> str:
    """
    Decrypts and extracts the Zalo backup file. Requires a valid decryption key.
    Shamelessly stolen from https://github.com/beer-psi/zalo-decrypt-backup/.

    Returns the Zalo user ID of the backup.
    """
    key = hashlib.sha256(dkey).digest()

    if len(path.suffixes) == 1:
        iv = b"\x00" * 16
    else:
        iv = b"zie" + dkey[:13]

    cipher = AES.new(key, AES.MODE_CBC, iv)  # pyright: ignore[reportUnknownMemberType]

    fd, temp = tempfile.mkstemp()
    os.close(fd)

    with path.open("rb") as fi, open(temp, "wb") as fout:
        while True:
            chunk = fi.read(1048576)

            if not chunk:
                break

            _ = fout.write(cipher.decrypt(chunk))

    output = path

    while len(output.suffixes) > 0:
        output = output.with_suffix("")

    output.mkdir(parents=True, exist_ok=True)

    with tarfile.TarFile(temp) as tar:
        if sys.version_info >= (3, 12):
            tar.extractall(output, filter="data")
        else:
            tar.extractall(output)

    os.unlink(temp)

    return os.listdir(output)[0]


def load_zalo_conversations(path: str, conn: sqlite3.Connection):
    """Loads up Zalo conversations list from a zdb file into the DHT database."""
    cur = conn.cursor()
    with Path(path).open("r") as f:
        for line in f:
            entry = json.loads(line)

            id: str = entry["userId"]

            if id.startswith("g"):
                id_int = int(id.removeprefix("g"))
                cur.execute(
                    "INSERT INTO servers (id, name, type) VALUES (?, ?, ?) ON CONFLICT DO UPDATE SET name = excluded.name",
                    (id_int, f"Group #{id_int}", "GROUP"),
                )
                cur.execute(
                    "INSERT INTO channels (id, server, name) VALUES (?, ?, ?) ON CONFLICT DO UPDATE SET name = excluded.name",
                    (id_int, id_int, f"Group #{id_int}"),
                )
            else:
                id_int = int(id)
                cur.execute(
                    "INSERT INTO servers (id, name, type) VALUES (?, ?, ?) ON CONFLICT DO UPDATE SET name = excluded.name",
                    (id_int, f"User #{id_int}", "DM"),
                )
                cur.execute(
                    "INSERT INTO channels (id, server, name) VALUES (?, ?, ?) ON CONFLICT DO UPDATE SET name = excluded.name",
                    (id_int, id_int, f"User #{id_int}"),
                )

            conn.commit()


def load_zalo_messages(path: str, conn: sqlite3.Connection):
    """Loads up Zalo messages data from a zdb file into the DHT database."""
    cur = conn.cursor()
    with Path(path).open("r") as f:
        for line in f:
            entry = json.loads(line)

            match entry["msgType"]:
                case 1 | 20:
                    message_text = entry["message"]

                    if not isinstance(message_text, str):
                        if message_text.get("action") == "rtf":
                            message_text = message_text["title"]
                        elif entry["msgType"] == 20:  # some weird catId: 0, id: 0 shit
                            message_text = "[Tin nhắn đã bị thu hồi]"
                        else:
                            raise ValueError(
                                f"Do not know how to handle msgType={entry['msgType']!r} message={entry['message']!r}"
                            )

                    cur.execute(
                        "INSERT INTO users (id, name, display_name, avatar_url, discriminator) VALUES (?, ?, NULL, NULL, NULL) ON CONFLICT DO UPDATE SET name = IIF(name LIKE 'User #%', excluded.name, name)",
                        (
                            int(entry["fromUid"]),
                            entry["dName"]
                            if entry["dName"]
                            else f"User #{entry['fromUid']}",
                        ),
                    )

                    cur.execute(
                        "INSERT INTO messages (message_id, sender_id, channel_id, text, timestamp) VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
                        (
                            int(entry["cliMsgId"]),
                            int(entry["fromUid"]),
                            int(entry["toUid"].removeprefix("g")),
                            message_text,
                            int(entry["serverTime"])
                        ),
                    )
                case 3: # voice message
                    pass
                case 4: # sticker
                    pass
                case 6: # link
                    pass
                case 7: # gif
                    pass
                case 17: # location
                    pass
                case 18: # video
                    pass
                case 19: # file
                    pass
                case 21: # embed
                    pass
                case 25: # accepted friend request header
                    pass
                case 26: # poll
                    pass
                case 52: # zinstant data
                    pass
                case -1909: # pinned message
                    pass
                case -27: # deleted message
                    pass
                case -4: # join / leave group
                    pass
                case _:
                    pass

            conn.commit()


def main():
    parser = argparse.ArgumentParser()
    _ = parser.add_argument("dkey", type=str.encode, help="Decryption key")
    _ = parser.add_argument(
        "path", type=Path, help="Path to encrypted Zalo backup file"
    )
    _ = parser.add_argument("database", type=Path, help="Path to DHT database file")
    _ = parser.add_argument(
        "-i",
        "--index",
        type=Path,
        help="Path to Zalo Index.db file for populating user and group names",
    )

    args = parser.parse_args()

    dkey: bytes = args.dkey
    path: Path = args.path
    database: Path = args.database
    index: Optional[Path] = args.index

    uid = extract_zalo_backup(dkey, path)

    conn = connect_to_database(database)

    while len(path.suffixes) > 0:
        path = path.with_suffix("")

    # volatile as fuck, you can easily point to the wrong zdb file here and we wouldn't know, because these are newline delimited JSON objects.
    load_zalo_conversations(
        os.path.join(
            path, uid, "ZaloDownloads", "database", f"{uid}_zconversation.zdb"
        ),
        conn,
    )
    load_zalo_messages(
        os.path.join(path, uid, "ZaloDownloads", "database", f"{uid}_zmessage.zdb"),
        conn,
    )


if __name__ == "__main__":
    main()
