"""从 RSS 与新闻 sitemap 采集新闻 URL。

每次运行只把此前未出现过的 URL 追加进 ``data/*_urls.json``，历史数组原样保留。
同一模块还负责把语料导出为 Parquet。
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
from xml.etree import ElementTree as ET

import feedparser
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent
# 新闻源配置与脚本放在同一目录，避免依赖当前工作目录
SITES_PATH = ROOT / "sites.json"


# --------------------------------------------------------------------- 抓取


def normalize_url(url: str) -> str:
    """规范化 URL：保留 scheme、host 与 path，丢弃 query 与 fragment。

    Args:
        url: 待规范化的绝对 URL。

    Returns:
        去掉 query 与 fragment、host 转小写后的 URL。

    Raises:
        ValueError: 该值不是绝对 HTTP URL。
    """
    parts = urlsplit(url.strip())
    if parts.scheme not in {"http", "https"} or not parts.hostname:
        raise ValueError(f"not an HTTP URL: {url!r}")
    return urlunsplit((parts.scheme, parts.netloc.lower(), parts.path, "", ""))


def parse_feed(payload: bytes, kind: str) -> list[str]:
    """解析文档并返回文章 URL，拒绝格式错误或内容为空的源文档。

    Args:
        payload: 响应原始字节。
        kind: ``rss`` 或 ``news_sitemap``。

    Returns:
        按文档顺序排列、已规范化并去重的文章 URL。

    Raises:
        ValueError: 文档格式错误、kind 未知或未解析出任何 URL。
    """
    if kind == "rss":
        feed = feedparser.parse(payload)
        # bozo 为真说明 XML 结构损坏，通常是错误页或截断响应
        if feed.bozo:
            raise ValueError(f"malformed RSS: {feed.bozo_exception}")
        urls = [entry.link for entry in feed.entries if entry.get("link")]
    elif kind == "news_sitemap":
        # 先拒绝 DOCTYPE / ENTITY，避免 XXE 与实体膨胀攻击
        if b"<!DOCTYPE" in payload.upper() or b"<!ENTITY" in payload.upper():
            raise ValueError("XML declarations are not allowed in sitemaps")
        root = ET.fromstring(payload)  # noqa: S314 上方已拒绝 DOCTYPE/ENTITY
        # 去掉命名空间后根节点必须是 urlset
        if root.tag.rsplit("}", 1)[-1] != "urlset":
            raise ValueError("expected a news sitemap urlset")
        urls = [node.text for node in root.findall("{*}url/{*}loc") if node.text]
    else:
        raise ValueError(f"unknown source kind: {kind}")
    if not urls:
        raise ValueError("source contained no URLs")
    return list(dict.fromkeys(normalize_url(url) for url in urls))


def make_session() -> requests.Session:
    """创建带重试上限和显式超时的 HTTP 会话。

    Returns:
        对瞬时故障重试两次并带退避的会话。
    """
    session = requests.Session()
    # UA 里带上项目地址，方便对方站点识别并联系
    session.headers["User-Agent"] = (
        "top-news/1.0 (+https://github.com/notnews/top_news)"
    )
    retry = Retry(total=2, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    for scheme in ("https://", "http://"):
        session.mount(scheme, HTTPAdapter(max_retries=retry))
    return session


def fetch_site(session: requests.Session, site: dict) -> list[str]:
    """收集可用的 feed；只有该站所有 feed 都失败时才判定整站失败。

    Args:
        session: 所有请求共用的 HTTP 会话。
        site: 含 ``kind`` 与 ``urls`` 列表的站点配置。

    Returns:
        所有可用 feed 汇总、去重后的文章 URL。

    Raises:
        ValueError: 配置的 feed 全部失败或全部为空。
    """
    urls = []
    successes = 0
    for feed_url in site["urls"]:
        try:
            # 单个 feed 超时 30 秒，避免一个地址拖住整轮采集
            response = session.get(feed_url, timeout=30)
            response.raise_for_status()
            urls.extend(parse_feed(response.content, site["kind"]))
            successes += 1
        except (requests.RequestException, ValueError, ET.ParseError) as exc:
            # 单个 feed 失败只告警，继续尝试该站的其余 feed
            log.warning("%s: %s", feed_url, exc)
    if not successes:
        raise ValueError("all feeds failed or were empty")
    return list(dict.fromkeys(urls))


# --------------------------------------------------------------------- 存储


def load_urls(path: Path) -> list[str]:
    """读取 JSON URL 数组，拒绝其他数据结构。

    Args:
        path: 待读取的文件；文件不存在时返回空数组。

    Returns:
        按原顺序存储的 URL。

    Raises:
        ValueError: 文件内容不是字符串数组。
    """
    values = json.loads(path.read_text(encoding="utf-8")) if path.exists() else []
    if not isinstance(values, list) or any(not isinstance(url, str) for url in values):
        raise ValueError(f"expected a URL array: {path}")
    return values


def write_urls(path: Path, urls: list[str]) -> None:
    """通过同目录临时文件写入，每行一条 URL。

    Args:
        path: 目标文件。
        urls: 待序列化的 URL。
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    part = path.with_suffix(path.suffix + ".part")
    payload = json.dumps(urls, indent=0, ensure_ascii=False) + "\n"
    # 先写 .part 再原子替换，进程中断也不会留下半截 JSON
    part.write_text(payload, encoding="utf-8")
    part.replace(path)


def append_urls(path: Path, incoming: list[str]) -> tuple[int, int]:
    """追加新的规范化 URL，保留既有内容与顺序。

    Args:
        path: 保存历史数组的文件。
        incoming: 本次发现的候选 URL。

    Returns:
        ``(新增数, 总数)`` 二元组。
    """
    urls = load_urls(path)
    seen = set()
    invalid = 0
    for url in urls:
        try:
            seen.add(normalize_url(url))
        except ValueError:
            # 历史脏值只计数、只告警，绝不删除或改写
            invalid += 1
    if invalid:
        log.warning(
            "%s: preserving %d historical values that are not absolute URLs",
            path,
            invalid,
        )
    added = 0
    for url in incoming:
        normalized = normalize_url(url)
        if normalized not in seen:
            seen.add(normalized)
            urls.append(normalized)
            added += 1
    # 没有新增就不写盘，保证文件内容与修改时间不变
    if added or not path.exists():
        write_urls(path, urls)
    return added, len(urls)


# --------------------------------------------------------------------- 文档

TABLE_HEADER = "| 文件 | URL 数 |"
TABLE_ALIGN = "|---|---:|"
README_NOTE = "各数组位于 `data/` 目录，每行一条 URL。下表为当前实测数量，合计 **{total:,}** 条。"
DATA_HEADING = "## 数据"
UPDATE_PREFIX = "更新时间 "
# README 里的时间统一按 UTC+8 展示
CN_TZ = timezone(timedelta(hours=8))


def now_cn() -> str:
    """返回 UTC+8 的当前时间，格式为 ``YYYY-MM-DD HH:MM:SS``。

    Returns:
        形如 ``2026-10-17 22:46:24`` 的时间字符串。
    """
    return datetime.now(CN_TZ).strftime("%Y-%m-%d %H:%M:%S")


def render_table(directory: Path) -> tuple[list[str], int]:
    """按目录中的实际条数生成 Markdown 表格。

    Args:
        directory: 存放 ``*_urls.json`` 的目录。

    Returns:
        ``(表格行列表, 总条数)`` 二元组。
    """
    rows = []
    total = 0
    for path in sorted(directory.glob("*_urls.json")):
        count = len(load_urls(path))
        total += count
        rows.append(f"| `{path.name}` | {count:,} |")
    rows.append(f"| **合计** | **{total:,}** |")
    return [TABLE_HEADER, TABLE_ALIGN, *rows], total


def update_readme(directory: Path, readme: Path | None = None) -> bool:
    """把 README 的说明句与数据表同步为当前计数。

    这是采集的附属动作：README 缺失、不可读或不可写时只记 warning 并跳过，
    绝不让写文档的失败影响采集结果。

    Args:
        directory: 存放 ``*_urls.json`` 的目录。
        readme: README 路径；默认取 ``directory`` 的上一级。

    Returns:
        文件内容确实发生变化并写回时为 True；跳过或失败时为 False。
    """
    readme = readme or directory.parent / "README.md"
    # 不存在（或不是普通文件）就跳过：计数只是附属信息，不能因此中断采集
    if not readme.is_file():
        return False
    # 目录为空时不动 README，避免把表格刷成 0
    if not any(directory.glob("*_urls.json")):
        return False
    try:
        lines = readme.read_text(encoding="utf-8").splitlines()
        table, total = render_table(directory)

        # 「## 数据」到说明句之间保持紧凑：标题、更新时间、说明句三行相连
        try:
            heading = lines.index(DATA_HEADING)
            note_index = next(
                i for i, line in enumerate(lines) if line.startswith("各数组位于")
            )
        except (ValueError, StopIteration):
            return False
        if note_index <= heading:
            return False
        lines[note_index] = README_NOTE.format(total=total)
        del lines[heading + 1 : note_index]
        lines.insert(heading + 1, f"{UPDATE_PREFIX}{now_cn()}")

        try:
            start = lines.index(TABLE_HEADER)
        except ValueError:
            # README 没有预期表头就不改写，避免破坏其他结构
            return False
        end = start
        while end < len(lines) and lines[end].startswith("|"):
            end += 1
        lines[start:end] = table
        payload = "\n".join(lines) + "\n"
        if payload == readme.read_text(encoding="utf-8"):
            return False
        readme.write_text(payload, encoding="utf-8")
    except (OSError, ValueError) as exc:
        # 只读、权限不足、编码异常、JSON 损坏等都走这里：告警后放弃本次同步
        log.warning("README 计数未更新（%s）：%s", readme, exc)
        return False
    return True


# --------------------------------------------------------------------- 配置


def load_sites(path: Path = SITES_PATH) -> dict[str, dict]:
    """读取并校验新闻源配置。

    Args:
        path: 站点代号到 ``kind`` 与 feed URL 列表的 JSON 映射。

    Returns:
        校验通过的配置，保持文档中的顺序。

    Raises:
        ValueError: 文档不是非空对象，或某个条目缺少合法的 ``kind`` /
            非空字符串 URL 列表。
    """
    with path.open(encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, dict) or not data:
        raise ValueError(f"expected a non-empty object: {path}")
    for name, site in data.items():
        if not isinstance(site, dict):
            raise ValueError(f"{name}: expected an object")
        if site.get("kind") not in {"rss", "news_sitemap"}:
            raise ValueError(f"{name}: unknown kind {site.get('kind')!r}")
        urls = site.get("urls")
        if (
            not isinstance(urls, list)
            or not urls
            or not all(isinstance(url, str) and url for url in urls)
        ):
            raise ValueError(f"{name}: urls must be a non-empty list of strings")
    return data


# 导入时即加载一次，配置有错立刻失败，不带病运行
SITES = load_sites()


# ------------------------------------------------------------------- 命令行


def collect(directory: Path, names: list[str], session=None) -> int:
    """逐个站点采集并打印统计；仅当所有站点都失败时返回非零。

    Args:
        directory: 存放 ``*_urls.json`` 的目录。
        names: 要采集的站点代号。
        session: 可选的现成会话，主要用于测试。

    Returns:
        至少一个站点成功时返回 0，全部失败返回 1。
    """
    session = session or make_session()
    succeeded = 0
    sys.stdout.write("site        added       total  status\n")
    for name in names:
        try:
            incoming = fetch_site(session, SITES[name])
            added, total = append_urls(directory / f"{name}_urls.json", incoming)
        except Exception as exc:
            # 单站异常只打印一行 failed，继续处理其余站点
            log.warning("%s: %s", name, exc)
            sys.stdout.write(f"{name:<10}       -           -  failed\n")
            continue
        succeeded += 1
        sys.stdout.write(f"{name:<10} {added:>6} {total:>11}  ok\n")
    # 数据已落盘，无论成败都按当前总数同步 README；
    # 文档更新属于附属动作，任何异常都不能影响采集结果与退出码
    try:
        update_readme(directory)
    except Exception as exc:  # noqa: BLE001 兜底：文档失败只告警
        log.warning("README 计数同步失败：%s", exc)
    return int(not succeeded)


def main(argv: list[str] | None = None) -> int:
    """执行采集或导出命令。

    Args:
        argv: 命令行参数；默认为 ``sys.argv[1:]``。

    Returns:
        进程退出码。
    """
    parser = argparse.ArgumentParser(prog="top_news.py")
    sub = parser.add_subparsers(dest="command", required=True)
    command = sub.add_parser("update")
    command.add_argument("--site", choices=sorted(SITES))
    command.add_argument("--directory", type=Path, default=Path("data"))
    command = sub.add_parser("to-parquet")
    command.add_argument("--directory", type=Path, default=Path("data"))
    command.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    if args.command == "update":
        return collect(args.directory, [args.site] if args.site else list(SITES))

    # pyarrow 只在导出时需要，放到函数内避免采集时加载这个重依赖
    import pyarrow as pa
    import pyarrow.parquet as pq

    rows = [
        {"site": name, "url": url}
        for name in SITES
        for url in dict.fromkeys(load_urls(args.directory / f"{name}_urls.json"))
    ]
    schema = pa.schema(
        [
            pa.field("site", pa.string(), nullable=False),
            pa.field("url", pa.string(), nullable=False),
        ]
    )
    args.out.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pylist(rows, schema=schema), args.out, compression="zstd"
    )
    sys.stdout.write(f"rows: {len(rows)}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
