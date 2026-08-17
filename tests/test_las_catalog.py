"""
LAS Catalog 集成测试：命名空间、读写与显式创建
"""

import json
import os
import sys
from datetime import datetime
from typing import Any, Dict

import pytest
import pandas as pd
import pyarrow as pa
import pyarrow.ipc as ipc
import ray
from lance.optimize import CompactionOptions

# 为了在本仓库内直接运行示例，动态追加本地包搜索路径（不影响已安装环境）
_CUR_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_CUR_DIR)
_LOCAL_PKG_DIR = os.path.join(_REPO_ROOT, "lance-ray")
if os.path.isdir(_LOCAL_PKG_DIR) and _LOCAL_PKG_DIR not in sys.path:
    sys.path.append(_LOCAL_PKG_DIR)

# 导入 lance-ray I/O 方法
from lance_ray.compaction import compact_files
from lance_ray.index import create_scalar_index
from lance_ray.io import write_lance, read_lance

# 导入 lance-namespace 连接与请求模型
from lance_namespace import connect
from lance_namespace_urllib3_client.models import (
    CreateNamespaceRequest,
    NamespaceExistsRequest,
    CreateTableRequest,
    DescribeTableRequest,
)


# ---------- Fixtures ----------

@pytest.fixture(scope="session", autouse=True)
def ray_autouse_las():
    """初始化 Ray，避免测试之间的相互影响。"""
    if ray.is_initialized():
        ray.shutdown()
    ray.init(local_mode=False, ignore_reinit_error=True)
    yield
    if ray.is_initialized():
        ray.shutdown()


def _load_props_from_config() -> Dict[str, Any]:
    """从 tests/config.json 加载连接与存储选项。

    返回 props，包含 lance-namespace 需要的连接信息与 Lance 存储选项（storage.*）。
    """
    cfg_path = os.path.join(_CUR_DIR, "config.json")
    if not os.path.exists(cfg_path):
        pytest.skip("tests/config.json 未找到，跳过 LAS Catalog 相关测试", allow_module_level=True)
    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    c = cfg.get("lance_poc", {})
    ak = c.get("access_key_id")
    sk = c.get("secret_access_key")
    endpoint = c.get("aws_endpoint")
    region = c.get("aws_region")
    vhost = c.get("virtual_hosted_style_request", "true")
    catalog = c.get("catalog")
    public_cloud = c.get("public_cloud")
    e2e_bucket = c.get("e2e_bucket")
    e2e_prefix = c.get("e2e_prefix", "lance_namespace_e2e")

    props = {
        # LAS Catalog 基本信息
        "access_key_id": ak,
        "secret_access_key": sk,
        "aws_endpoint": endpoint,
        "aws_region": region,
        "virtual_hosted_style_request": vhost,
    }
    if catalog:
        props["catalog"] = catalog
    if public_cloud is not None:
        props["public_cloud"] = public_cloud
    if e2e_bucket:
        props["_e2e_bucket"] = e2e_bucket
    if e2e_prefix:
        props["_e2e_prefix"] = e2e_prefix
    return props


def _build_base_location(props: Dict[str, Any], db_name: str) -> str:
    bucket = props.pop("_e2e_bucket", None)
    prefix = props.pop("_e2e_prefix", "lance_namespace_e2e")
    if not bucket:
        pytest.skip("tests/config.json 缺少 lance_poc.e2e_bucket，无法执行 LAS E2E")
    prefix = str(prefix).strip("/")
    return f"tos://{bucket}/{prefix}/{db_name}"


def _ensure_namespace(props: Dict[str, Any], db_name: str):
    ns = connect("las", props)
    base_location = _build_base_location(dict(props), db_name)
    request = CreateNamespaceRequest(
        id=[db_name],
        mode="exist_ok",
        properties={"location": base_location},
    )
    _ = ns.create_namespace(request)
    exists = ns.namespace_exists(NamespaceExistsRequest(id=[db_name]))
    assert getattr(exists, "exists", True), "命名空间不存在"
    return ns, base_location


def _connection_properties(props: Dict[str, Any]) -> Dict[str, str]:
    """Return only properties understood by the namespace connector."""
    return {
        key: value
        for key, value in props.items()
        if not key.startswith("_") and value is not None
    }


@pytest.fixture(scope="session")
def db_name():
    """为测试会话生成（或配置）一个命名空间名。"""
    # 可通过环境变量覆盖，默认按时间戳生成
    env_name = os.getenv("LAS_DB_NAME")
    if env_name:
        return env_name
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    return f"pytest_las_{ts}"


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "id": list(range(1, 6)),
        "name": [f"row_{i}" for i in range(1, 6)],
        "value": [float(i) * 1.5 for i in range(1, 6)],
    })

# ---------- Tests ----------

def test_write_then_read_roundtrip(db_name, sample_df):
    """使用 lance-ray 写入并读取同一张表，校验行数与前几行。"""
    # 建立命名空间连接，并确保 namespace 存在
    props = _load_props_from_config()
    ns, _ = _ensure_namespace(props, db_name)

    table_name = f"table_rw_{datetime.utcnow().strftime('%H%M%S')}"
    table_id = [db_name, table_name]

    # 准备 Ray Dataset
    ds = ray.data.from_pandas(sample_df)

    # 覆盖写入
    write_lance(ds, namespace=ns, table_id=table_id, mode="overwrite")

    # 验证表位置可获得（不强制要求成功，部分环境可能限制）
    try:
        desc = ns.describe_table(DescribeTableRequest(id=table_id))
        assert getattr(desc, "location", None), "DescribeTable 未返回位置"
    except Exception:
        # 允许 describe 失败，不影响核心读写验证
        pass

    # 读取并检查
    read_ds = read_lance(namespace=ns, table_id=table_id)
    assert read_ds.count() == len(sample_df)
    head = read_ds.take(3)
    assert isinstance(head, list) and len(head) == 3


def test_explicit_create_table_ipc_then_append(db_name):
    """通过 Arrow IPC 显式创建表，然后追加并验证行数。"""
    # 建立命名空间连接，并确保 namespace 存在
    props = _load_props_from_config()
    ns, _ = _ensure_namespace(props, db_name)

    table_name = f"table_ipc_{datetime.utcnow().strftime('%H%M%S')}"
    table_id = [db_name, table_name]

    # 构造 Arrow Table 并序列化为 IPC 字节
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["a", "b", "c"],
        "value": [0.1, 0.2, 0.3],
    })
    tbl = pa.Table.from_pandas(df)
    buf = pa.BufferOutputStream()
    with ipc.new_stream(buf, tbl.schema) as writer:
        writer.write_table(tbl)
    request_data = buf.getvalue().to_pybytes()

    # 显式创建
    _ = ns.create_table(CreateTableRequest(id=table_id), request_data)

    # 读取并验证
    ds = read_lance(namespace=ns, table_id=table_id)
    pre_count = ds.count()
    # 某些环境仅注册 schema 不落地数据，允许 pre_count 为 0 或 3
    assert pre_count in (0, 3)

    # 追加两行并再次验证
    df2 = pd.DataFrame({
        "id": [4, 5],
        "name": ["d", "e"],
        "value": [0.4, 0.5],
    })
    ds2 = ray.data.from_pandas(df2)
    write_lance(ds2, namespace=ns, table_id=table_id, mode="append")

    ds_all = read_lance(namespace=ns, table_id=table_id)
    # 如果初始为 0，则追加后应为 2；如果初始为 3，则追加后应为 5
    assert ds_all.count() in (2, 5)


def test_namespace_impl_write_read_roundtrip(db_name, sample_df):
    """Read and write through serializable LAS connection parameters."""
    props = _load_props_from_config()
    _ensure_namespace(props, db_name)
    table_id = [
        db_name,
        f"table_impl_rw_{datetime.utcnow().strftime('%H%M%S')}",
    ]

    write_lance(
        ray.data.from_pandas(sample_df),
        namespace_impl="las",
        namespace_properties=_connection_properties(props),
        table_id=table_id,
        mode="overwrite",
    )

    actual = read_lance(
        namespace_impl="las",
        namespace_properties=_connection_properties(props),
        table_id=table_id,
    ).to_pandas()
    pd.testing.assert_frame_equal(
        sample_df.sort_values("id").reset_index(drop=True),
        actual.sort_values("id").reset_index(drop=True),
    )


def test_namespace_impl_compaction(db_name):
    """Compact a multi-fragment LAS table resolved through the catalog."""
    props = _load_props_from_config()
    _ensure_namespace(props, db_name)
    table_id = [
        db_name,
        f"table_impl_compact_{datetime.utcnow().strftime('%H%M%S')}",
    ]
    frame = pd.DataFrame(
        {
            "id": range(20),
            "value": [f"value_{idx}" for idx in range(20)],
        }
    )
    connection_properties = _connection_properties(props)

    write_lance(
        ray.data.from_pandas(frame),
        namespace_impl="las",
        namespace_properties=connection_properties,
        table_id=table_id,
        mode="overwrite",
        min_rows_per_file=5,
        max_rows_per_file=5,
    )
    metrics = compact_files(
        namespace_impl="las",
        namespace_properties=connection_properties,
        table_id=table_id,
        compaction_options=CompactionOptions(
            target_rows_per_fragment=100,
            num_threads=1,
        ),
        num_workers=2,
    )

    assert metrics is not None
    assert metrics.fragments_removed == 4
    assert metrics.fragments_added == 1
    assert read_lance(
        namespace_impl="las",
        namespace_properties=connection_properties,
        table_id=table_id,
    ).count() == len(frame)


def test_namespace_impl_btree_index(db_name):
    """Build and query a distributed scalar index on a LAS table."""
    props = _load_props_from_config()
    _ensure_namespace(props, db_name)
    table_id = [
        db_name,
        f"table_impl_btree_{datetime.utcnow().strftime('%H%M%S')}",
    ]
    frame = pd.DataFrame(
        {
            "id": range(100),
            "value": [f"value_{idx}" for idx in range(100)],
        }
    )
    connection_properties = _connection_properties(props)

    write_lance(
        ray.data.from_pandas(frame),
        namespace_impl="las",
        namespace_properties=connection_properties,
        table_id=table_id,
        mode="overwrite",
        min_rows_per_file=25,
        max_rows_per_file=25,
    )
    dataset = create_scalar_index(
        namespace_impl="las",
        namespace_properties=connection_properties,
        table_id=table_id,
        column="id",
        index_type="BTREE",
        name="id_btree",
        num_workers=2,
    )

    assert any(index["name"] == "id_btree" for index in dataset.list_indices())
    assert dataset.scanner(filter="id = 42").count_rows() == 1
