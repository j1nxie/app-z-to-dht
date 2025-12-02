import argparse
import hashlib
import json
import os
import sqlite3
import sys
import tarfile
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Optional
from urllib.parse import urlparse

if TYPE_CHECKING:
    import sqlite3 as sqlcipher3
else:
    import sqlcipher3
from Crypto.Cipher import AES


def connect_to_database(path: Path):
    """Connects to the DHT SQLite database. Expects an existing DHT database."""
    conn = sqlite3.connect(path)
    conn.execute(
        "ATTACH DATABASE ? AS downloads", (str(path.with_suffix(".dht_downloads")),)
    )
    cur = conn.cursor()

    res = cur.execute("SELECT value FROM metadata WHERE key = 'version'")

    if res.fetchone():
        return conn
    else:
        raise ValueError("Database is not initialized.")


def connect_to_index(dkey: str, path: Path):
    conn = sqlcipher3.connect(path)
    cur = conn.cursor()

    _ = cur.execute(f"PRAGMA key = '{dkey}'")

    return conn


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


def load_zalo_conversations(
    path: str, conn: sqlite3.Connection, index: Optional[sqlite3.Connection] = None
):
    """Loads up Zalo conversations list from a zdb file into the DHT database."""
    cur = conn.cursor()

    with Path(path).open("r") as f:
        for line in f:
            entry = json.loads(line)

            id: str = entry["userId"]

            if id.startswith("g"):
                id_int = int(id.removeprefix("g"))

                if index:
                    res = index.execute(
                        "SELECT displayName FROM 'group' WHERE userId = ?", (id,)
                    )
                    row = res.fetchone()
                    name: str = row[0] if row else f"Group #{id_int}"
                else:
                    name = f"Group #{id_int}"

                cur.execute(
                    "INSERT INTO servers (id, name, type) VALUES (?, ?, ?) ON CONFLICT DO UPDATE SET name = excluded.name",
                    (id_int, name, "GROUP"),
                )
                cur.execute(
                    "INSERT INTO channels (id, server, name) VALUES (?, ?, ?) ON CONFLICT DO UPDATE SET name = excluded.name",
                    (id_int, id_int, name),
                )
            else:
                id_int = int(id)

                if index:
                    res = index.execute(
                        "SELECT displayName FROM friend WHERE userId = ? UNION SELECT displayName FROM friends_info WHERE userId = ?",
                        (id, id),
                    )
                    row = res.fetchone()
                    name = row[0] if row else f"User #{id_int}"
                else:
                    name = f"User #{id_int}"

                cur.execute(
                    "INSERT INTO servers (id, name, type) VALUES (?, ?, ?) ON CONFLICT DO UPDATE SET name = excluded.name",
                    (id_int, name, "DM"),
                )
                cur.execute(
                    "INSERT INTO channels (id, server, name) VALUES (?, ?, ?) ON CONFLICT DO UPDATE SET name = excluded.name",
                    (id_int, id_int, name),
                )

            conn.commit()


def load_zalo_messages(
    uid: str, path: Path, conn: sqlite3.Connection, index: Optional[sqlite3.Connection]
):
    """Loads up Zalo messages data from a zdb file into the DHT database."""
    cur = conn.cursor()
    with Path(
        os.path.join(path, uid, "ZaloDownloads", "database", f"{uid}_zmessage.zdb")
    ).open("r") as f:
        for line in f:
            entry = json.loads(line)

            if entry["quote"] is not None:
                message_quote_owner_id = int(
                    entry["quote"]["ownerId"]
                    if entry["quote"]["ownerId"] != "0"
                    else uid
                )

                conn.execute(
                    "INSERT INTO message_replied_to (message_id, replied_to_id) VALUES (?, ?) ON CONFLICT DO NOTHING",
                    (int(entry["cliMsgId"]), entry["quote"]["cliMsgId"]),
                )
                conn.execute(
                    "INSERT INTO users (id, name, display_name, avatar_url, discriminator) VALUES (?, ?, NULL, NULL, NULL) ON CONFLICT DO UPDATE SET name = IIF(name LIKE 'User #%', excluded.name, name)",
                    (
                        message_quote_owner_id,
                        entry["quote"]["fromD"]
                        if "fromD" in entry["quote"] and entry["quote"]["fromD"]
                        else f"User #{message_quote_owner_id}",
                    ),
                )

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

                    if entry["dName"]:
                        name = entry["dName"]
                    elif index:
                        res = index.execute(
                            "SELECT displayName FROM friend WHERE userId = ? UNION SELECT displayName FROM friends_info WHERE userId = ?",
                            (entry["fromUid"], entry["fromUid"]),
                        )
                        row = res.fetchone()
                        name = row[0] if row else f"User #{entry['fromUid']}"
                    else:
                        name = f"User #{entry['fromUid']}"

                    cur.execute(
                        "INSERT INTO users (id, name, display_name, avatar_url, discriminator) VALUES (?, ?, NULL, NULL, NULL) ON CONFLICT DO UPDATE SET name = IIF(name LIKE 'User #%', excluded.name, name)",
                        (int(entry["fromUid"]), name),
                    )

                    cur.execute(
                        "INSERT INTO messages (message_id, sender_id, channel_id, text, timestamp) VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
                        (
                            int(entry["cliMsgId"]),
                            int(entry["fromUid"]),
                            int(entry["toUid"].removeprefix("g")),
                            message_text,
                            int(entry["serverTime"]),
                        ),
                    )
                case 2:  # image
                    cur.execute(
                        "INSERT INTO messages (message_id, sender_id, channel_id, text, timestamp) VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
                        (
                            int(entry["cliMsgId"]),
                            int(entry["fromUid"]),
                            int(entry["toUid"].removeprefix("g")),
                            "",
                            int(entry["serverTime"]),
                        ),
                    )

                    params = json.loads(entry["message"]["params"])
                    image_url: str = entry["message"]["oriUrl"]
                    image_name = os.path.basename(urlparse(image_url).path)
                    image_type = os.path.splitext(image_name)[1][1:].lower()

                    image_hash = hashlib.md5(image_url.encode()).hexdigest()
                    saved_image_path = os.path.join(
                        path,
                        uid,
                        "ZaloDownloads",
                        "picture",
                        f"{entry['toUid'].removeprefix('g')}{'_group' if entry['toUid'].startswith('g') else ''}",
                        f"z{entry['msgId']}_{image_hash}.{image_type}",
                    )

                    if os.path.exists(saved_image_path):
                        size = os.stat(saved_image_path).st_size

                        with open(saved_image_path, "rb") as f:
                            blob = f.read()
                    else:
                        size = 0
                        blob = None

                    cur.execute(
                        "INSERT INTO attachments (attachment_id, name, type, normalized_url, download_url, size, width, height) VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
                        (
                            int(entry["cliMsgId"]),
                            image_name,
                            f"image/{'jpeg' if image_type == 'jpg' else image_type}",
                            image_url,
                            image_url,
                            size,
                            params.get("width"),
                            params.get("height"),
                        ),
                    )

                    cur.execute(
                        "INSERT INTO message_attachments (message_id, attachment_id) VALUES (?, ?) ON CONFLICT DO NOTHING",
                        (
                            int(entry["cliMsgId"]),
                            int(entry["cliMsgId"]),
                        ),
                    )

                    if blob:
                        cur.execute(
                            "INSERT INTO download_blobs (normalized_url, blob) VALUES (?, ?) ON CONFLICT DO NOTHING",
                            (image_url, blob),
                        )

                        cur.execute(
                            "INSERT INTO download_metadata (normalized_url, download_url, status, type, size) VALUES (?, ?, 200, ?, ?) ON CONFLICT DO NOTHING",
                            (
                                image_url,
                                image_url,
                                f"image/{'jpeg' if image_type == 'jpg' else image_type}",
                                size,
                            ),
                        )
                case 3:  # voice message
                    pass
                case 4:  # sticker
                    pass
                case 6:  # link
                    pass
                case 7:  # gif
                    pass
                case 17:  # location
                    pass
                case 18:  # video
                    pass
                case 19:  # file
                    pass
                case 21:  # embed
                    pass
                case 25:  # accepted friend request header
                    pass
                case 26:  # poll
                    pass
                case 52:  # zinstant data
                    pass
                case -1909:  # pinned message
                    pass
                case -27:  # deleted message
                    pass
                case -4:  # join / leave group
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
    index_conn = connect_to_index(dkey.decode(), index) if index else None

    while len(path.suffixes) > 0:
        path = path.with_suffix("")

    # volatile as fuck, you can easily point to the wrong zdb file here and we wouldn't know, because these are newline delimited JSON objects.
    load_zalo_conversations(
        os.path.join(
            path, uid, "ZaloDownloads", "database", f"{uid}_zconversation.zdb"
        ),
        conn,
        index_conn,
    )
    load_zalo_messages(
        uid,
        path,
        conn,
        index_conn,
    )


if __name__ == "__main__":
    main()
