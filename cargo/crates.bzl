"""
@generated
cargo-raze generated Bazel file.

DO NOT EDIT! Replaced on runs of cargo-raze
"""

load("@bazel_tools//tools/build_defs/repo:git.bzl", "new_git_repository")  # buildifier: disable=load
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")  # buildifier: disable=load
load("@bazel_tools//tools/build_defs/repo:utils.bzl", "maybe")  # buildifier: disable=load

# EXPERIMENTAL -- MAY CHANGE AT ANY TIME: A mapping of package names to a set of normal dependencies for the Rust targets of that package.
_DEPENDENCIES = {
    "webhook": {
        "ed25519-dalek": "@raze__ed25519_dalek__1_0_1//:ed25519_dalek",
        "hex": "@raze__hex__0_4_3//:hex",
        "hyper": "@raze__hyper__0_14_12//:hyper",
        "lazy_static": "@raze__lazy_static__1_4_0//:lazy_static",
        "libc": "@raze__libc__0_2_101//:libc",
        "rand": "@raze__rand__0_7_3//:rand",
        "serde": "@raze__serde__1_0_130//:serde",
        "serde_json": "@raze__serde_json__1_0_67//:serde_json",
        "tokio": "@raze__tokio__1_11_0//:tokio",
    },
    "common/rust": {
        "config": "@raze__config__0_11_0//:config",
        "hyper": "@raze__hyper__0_14_12//:hyper",
        "log": "@raze__log__0_4_14//:log",
        "nats": "@raze__nats__0_15_2//:nats",
        "pretty_env_logger": "@raze__pretty_env_logger__0_4_0//:pretty_env_logger",
        "prometheus": "@raze__prometheus__0_12_0//:prometheus",
        "redis": "@raze__redis__0_21_2//:redis",
        "serde": "@raze__serde__1_0_130//:serde",
        "testcontainers": "@raze__testcontainers__0_12_0//:testcontainers",
        "tokio": "@raze__tokio__1_11_0//:tokio",
    },
    "gateway": {
        "enumflags2": "@raze__enumflags2__0_7_1//:enumflags2",
        "futures": "@raze__futures__0_3_17//:futures",
        "futures-util": "@raze__futures_util__0_3_17//:futures_util",
        "num": "@raze__num__0_4_0//:num",
        "num-traits": "@raze__num_traits__0_2_14//:num_traits",
        "serde": "@raze__serde__1_0_130//:serde",
        "serde_json": "@raze__serde_json__1_0_67//:serde_json",
        "tokio": "@raze__tokio__1_11_0//:tokio",
        "tokio-tungstenite": "@raze__tokio_tungstenite__0_15_0//:tokio_tungstenite",
        "url": "@raze__url__2_2_2//:url",
    },
    "cache": {
        "log": "@raze__log__0_4_14//:log",
        "nats": "@raze__nats__0_15_2//:nats",
        "redis": "@raze__redis__0_21_2//:redis",
        "serde": "@raze__serde__1_0_130//:serde",
        "serde_json": "@raze__serde_json__1_0_67//:serde_json",
    },
    "rest": {
        "futures-util": "@raze__futures_util__0_3_17//:futures_util",
        "hyper": "@raze__hyper__0_14_12//:hyper",
        "hyper-tls": "@raze__hyper_tls__0_5_0//:hyper_tls",
        "lazy_static": "@raze__lazy_static__1_4_0//:lazy_static",
        "serde": "@raze__serde__1_0_130//:serde",
        "tokio": "@raze__tokio__1_11_0//:tokio",
        "xxhash-rust": "@raze__xxhash_rust__0_8_2//:xxhash_rust",
    },
    "": {
        "libc": "@raze__libc__0_2_101//:libc",
    },
}

# EXPERIMENTAL -- MAY CHANGE AT ANY TIME: A mapping of package names to a set of proc_macro dependencies for the Rust targets of that package.
_PROC_MACRO_DEPENDENCIES = {
    "webhook": {
    },
    "common/rust": {
    },
    "gateway": {
        "num-derive": "@raze__num_derive__0_3_3//:num_derive",
        "serde_repr": "@raze__serde_repr__0_1_7//:serde_repr",
    },
    "cache": {
    },
    "rest": {
    },
    "": {
    },
}

# EXPERIMENTAL -- MAY CHANGE AT ANY TIME: A mapping of package names to a set of normal dev dependencies for the Rust targets of that package.
_DEV_DEPENDENCIES = {
    "webhook": {
    },
    "common/rust": {
    },
    "gateway": {
    },
    "cache": {
    },
    "rest": {
    },
    "": {
    },
}

# EXPERIMENTAL -- MAY CHANGE AT ANY TIME: A mapping of package names to a set of proc_macro dev dependencies for the Rust targets of that package.
_DEV_PROC_MACRO_DEPENDENCIES = {
    "webhook": {
    },
    "common/rust": {
    },
    "gateway": {
    },
    "cache": {
    },
    "rest": {
    },
    "": {
    },
}

def crate_deps(deps, package_name = None):
    """EXPERIMENTAL -- MAY CHANGE AT ANY TIME: Finds the fully qualified label of the requested crates for the package where this macro is called.

    WARNING: This macro is part of an expeirmental API and is subject to change.

    Args:
        deps (list): The desired list of crate targets.
        package_name (str, optional): The package name of the set of dependencies to look up.
            Defaults to `native.package_name()`.
    Returns:
        list: A list of labels to cargo-raze generated targets (str)
    """

    if not package_name:
        package_name = native.package_name()

    # Join both sets of dependencies
    dependencies = _flatten_dependency_maps([
        _DEPENDENCIES,
        _PROC_MACRO_DEPENDENCIES,
        _DEV_DEPENDENCIES,
        _DEV_PROC_MACRO_DEPENDENCIES,
    ])

    if not deps:
        return []

    missing_crates = []
    crate_targets = []
    for crate_target in deps:
        if crate_target not in dependencies[package_name]:
            missing_crates.append(crate_target)
        else:
            crate_targets.append(dependencies[package_name][crate_target])

    if missing_crates:
        fail("Could not find crates `{}` among dependencies of `{}`. Available dependencies were `{}`".format(
            missing_crates,
            package_name,
            dependencies[package_name],
        ))

    return crate_targets

def all_crate_deps(normal = False, normal_dev = False, proc_macro = False, proc_macro_dev = False, package_name = None):
    """EXPERIMENTAL -- MAY CHANGE AT ANY TIME: Finds the fully qualified label of all requested direct crate dependencies \
    for the package where this macro is called.

    If no parameters are set, all normal dependencies are returned. Setting any one flag will
    otherwise impact the contents of the returned list.

    Args:
        normal (bool, optional): If True, normal dependencies are included in the
            output list. Defaults to False.
        normal_dev (bool, optional): If True, normla dev dependencies will be
            included in the output list. Defaults to False.
        proc_macro (bool, optional): If True, proc_macro dependencies are included
            in the output list. Defaults to False.
        proc_macro_dev (bool, optional): If True, dev proc_macro dependencies are
            included in the output list. Defaults to False.
        package_name (str, optional): The package name of the set of dependencies to look up.
            Defaults to `native.package_name()`.

    Returns:
        list: A list of labels to cargo-raze generated targets (str)
    """

    if not package_name:
        package_name = native.package_name()

    # Determine the relevant maps to use
    all_dependency_maps = []
    if normal:
        all_dependency_maps.append(_DEPENDENCIES)
    if normal_dev:
        all_dependency_maps.append(_DEV_DEPENDENCIES)
    if proc_macro:
        all_dependency_maps.append(_PROC_MACRO_DEPENDENCIES)
    if proc_macro_dev:
        all_dependency_maps.append(_DEV_PROC_MACRO_DEPENDENCIES)

    # Default to always using normal dependencies
    if not all_dependency_maps:
        all_dependency_maps.append(_DEPENDENCIES)

    dependencies = _flatten_dependency_maps(all_dependency_maps)

    if not dependencies:
        return []

    return dependencies[package_name].values()

def _flatten_dependency_maps(all_dependency_maps):
    """Flatten a list of dependency maps into one dictionary.

    Dependency maps have the following structure:

    ```python
    DEPENDENCIES_MAP = {
        # The first key in the map is a Bazel package
        # name of the workspace this file is defined in.
        "package_name": {

            # An alias to a crate target.     # The label of the crate target the
            # Aliases are only crate names.   # alias refers to.
            "alias":                          "@full//:label",
        }
    }
    ```

    Args:
        all_dependency_maps (list): A list of dicts as described above

    Returns:
        dict: A dictionary as described above
    """
    dependencies = {}

    for dep_map in all_dependency_maps:
        for pkg_name in dep_map:
            if pkg_name not in dependencies:
                # Add a non-frozen dict to the collection of dependencies
                dependencies.setdefault(pkg_name, dict(dep_map[pkg_name].items()))
                continue

            duplicate_crate_aliases = [key for key in dependencies[pkg_name] if key in dep_map[pkg_name]]
            if duplicate_crate_aliases:
                fail("There should be no duplicate crate aliases: {}".format(duplicate_crate_aliases))

            dependencies[pkg_name].update(dep_map[pkg_name])

    return dependencies

def raze_fetch_remote_crates():
    """This function defines a collection of repos and should be called in a WORKSPACE file"""
    maybe(
        http_archive,
        name = "raze__adler__1_0_2",
        url = "https://crates.io/api/v1/crates/adler/1.0.2/download",
        type = "tar.gz",
        sha256 = "f26201604c87b1e01bd3d98f8d5d9a8fcbb815e8cedb41ffccbeb4bf593a35fe",
        strip_prefix = "adler-1.0.2",
        build_file = Label("//cargo/remote:BUILD.adler-1.0.2.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__aho_corasick__0_7_18",
        url = "https://crates.io/api/v1/crates/aho-corasick/0.7.18/download",
        type = "tar.gz",
        sha256 = "1e37cfd5e7657ada45f742d6e99ca5788580b5c529dc78faf11ece6dc702656f",
        strip_prefix = "aho-corasick-0.7.18",
        build_file = Label("//cargo/remote:BUILD.aho-corasick-0.7.18.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__arc_swap__1_4_0",
        url = "https://crates.io/api/v1/crates/arc-swap/1.4.0/download",
        type = "tar.gz",
        sha256 = "e6df5aef5c5830360ce5218cecb8f018af3438af5686ae945094affc86fdec63",
        strip_prefix = "arc-swap-1.4.0",
        build_file = Label("//cargo/remote:BUILD.arc-swap-1.4.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__arrayvec__0_5_2",
        url = "https://crates.io/api/v1/crates/arrayvec/0.5.2/download",
        type = "tar.gz",
        sha256 = "23b62fc65de8e4e7f52534fb52b0f3ed04746ae267519eef2a83941e8085068b",
        strip_prefix = "arrayvec-0.5.2",
        build_file = Label("//cargo/remote:BUILD.arrayvec-0.5.2.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__async_channel__1_6_1",
        url = "https://crates.io/api/v1/crates/async-channel/1.6.1/download",
        type = "tar.gz",
        sha256 = "2114d64672151c0c5eaa5e131ec84a74f06e1e559830dabba01ca30605d66319",
        strip_prefix = "async-channel-1.6.1",
        build_file = Label("//cargo/remote:BUILD.async-channel-1.6.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__async_task__4_0_3",
        url = "https://crates.io/api/v1/crates/async-task/4.0.3/download",
        type = "tar.gz",
        sha256 = "e91831deabf0d6d7ec49552e489aed63b7456a7a3c46cff62adad428110b0af0",
        strip_prefix = "async-task-4.0.3",
        build_file = Label("//cargo/remote:BUILD.async-task-4.0.3.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__async_trait__0_1_51",
        url = "https://crates.io/api/v1/crates/async-trait/0.1.51/download",
        type = "tar.gz",
        sha256 = "44318e776df68115a881de9a8fd1b9e53368d7a4a5ce4cc48517da3393233a5e",
        strip_prefix = "async-trait-0.1.51",
        build_file = Label("//cargo/remote:BUILD.async-trait-0.1.51.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__atomic_waker__1_0_0",
        url = "https://crates.io/api/v1/crates/atomic-waker/1.0.0/download",
        type = "tar.gz",
        sha256 = "065374052e7df7ee4047b1160cca5e1467a12351a40b3da123c870ba0b8eda2a",
        strip_prefix = "atomic-waker-1.0.0",
        build_file = Label("//cargo/remote:BUILD.atomic-waker-1.0.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__atty__0_2_14",
        url = "https://crates.io/api/v1/crates/atty/0.2.14/download",
        type = "tar.gz",
        sha256 = "d9b39be18770d11421cdb1b9947a45dd3f37e93092cbf377614828a319d5fee8",
        strip_prefix = "atty-0.2.14",
        build_file = Label("//cargo/remote:BUILD.atty-0.2.14.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__autocfg__1_0_1",
        url = "https://crates.io/api/v1/crates/autocfg/1.0.1/download",
        type = "tar.gz",
        sha256 = "cdb031dd78e28731d87d56cc8ffef4a8f36ca26c38fe2de700543e627f8a464a",
        strip_prefix = "autocfg-1.0.1",
        build_file = Label("//cargo/remote:BUILD.autocfg-1.0.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__base64__0_13_0",
        url = "https://crates.io/api/v1/crates/base64/0.13.0/download",
        type = "tar.gz",
        sha256 = "904dfeac50f3cdaba28fc6f57fdcddb75f49ed61346676a78c4ffe55877802fd",
        strip_prefix = "base64-0.13.0",
        build_file = Label("//cargo/remote:BUILD.base64-0.13.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__base64_url__1_4_10",
        url = "https://crates.io/api/v1/crates/base64-url/1.4.10/download",
        type = "tar.gz",
        sha256 = "44265cf903f576fcaa1c2f23b32ec2dadaa8ec9d6b7c6212704d72a417bfbeef",
        strip_prefix = "base64-url-1.4.10",
        build_file = Label("//cargo/remote:BUILD.base64-url-1.4.10.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__bitflags__1_3_2",
        url = "https://crates.io/api/v1/crates/bitflags/1.3.2/download",
        type = "tar.gz",
        sha256 = "bef38d45163c2f1dde094a7dfd33ccf595c92905c8f8f4fdc18d06fb1037718a",
        strip_prefix = "bitflags-1.3.2",
        build_file = Label("//cargo/remote:BUILD.bitflags-1.3.2.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__block_buffer__0_9_0",
        url = "https://crates.io/api/v1/crates/block-buffer/0.9.0/download",
        type = "tar.gz",
        sha256 = "4152116fd6e9dadb291ae18fc1ec3575ed6d84c29642d97890f4b4a3417297e4",
        strip_prefix = "block-buffer-0.9.0",
        build_file = Label("//cargo/remote:BUILD.block-buffer-0.9.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__blocking__1_0_2",
        url = "https://crates.io/api/v1/crates/blocking/1.0.2/download",
        type = "tar.gz",
        sha256 = "c5e170dbede1f740736619b776d7251cb1b9095c435c34d8ca9f57fcd2f335e9",
        strip_prefix = "blocking-1.0.2",
        build_file = Label("//cargo/remote:BUILD.blocking-1.0.2.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__bumpalo__3_7_0",
        url = "https://crates.io/api/v1/crates/bumpalo/3.7.0/download",
        type = "tar.gz",
        sha256 = "9c59e7af012c713f529e7a3ee57ce9b31ddd858d4b512923602f74608b009631",
        strip_prefix = "bumpalo-3.7.0",
        build_file = Label("//cargo/remote:BUILD.bumpalo-3.7.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__byteorder__1_4_3",
        url = "https://crates.io/api/v1/crates/byteorder/1.4.3/download",
        type = "tar.gz",
        sha256 = "14c189c53d098945499cdfa7ecc63567cf3886b3332b312a5b4585d8d3a6a610",
        strip_prefix = "byteorder-1.4.3",
        build_file = Label("//cargo/remote:BUILD.byteorder-1.4.3.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__bytes__1_1_0",
        url = "https://crates.io/api/v1/crates/bytes/1.1.0/download",
        type = "tar.gz",
        sha256 = "c4872d67bab6358e59559027aa3b9157c53d9358c51423c17554809a8858e0f8",
        strip_prefix = "bytes-1.1.0",
        build_file = Label("//cargo/remote:BUILD.bytes-1.1.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__cache_padded__1_1_1",
        url = "https://crates.io/api/v1/crates/cache-padded/1.1.1/download",
        type = "tar.gz",
        sha256 = "631ae5198c9be5e753e5cc215e1bd73c2b466a3565173db433f52bb9d3e66dba",
        strip_prefix = "cache-padded-1.1.1",
        build_file = Label("//cargo/remote:BUILD.cache-padded-1.1.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__cc__1_0_70",
        url = "https://crates.io/api/v1/crates/cc/1.0.70/download",
        type = "tar.gz",
        sha256 = "d26a6ce4b6a484fa3edb70f7efa6fc430fd2b87285fe8b84304fd0936faa0dc0",
        strip_prefix = "cc-1.0.70",
        build_file = Label("//cargo/remote:BUILD.cc-1.0.70.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__cfg_if__1_0_0",
        url = "https://crates.io/api/v1/crates/cfg-if/1.0.0/download",
        type = "tar.gz",
        sha256 = "baf1de4339761588bc0619e3cbc0120ee582ebb74b53b4efbf79117bd2da40fd",
        strip_prefix = "cfg-if-1.0.0",
        build_file = Label("//cargo/remote:BUILD.cfg-if-1.0.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__chrono__0_4_19",
        url = "https://crates.io/api/v1/crates/chrono/0.4.19/download",
        type = "tar.gz",
        sha256 = "670ad68c9088c2a963aaa298cb369688cf3f9465ce5e2d4ca10e6e0098a1ce73",
        strip_prefix = "chrono-0.4.19",
        build_file = Label("//cargo/remote:BUILD.chrono-0.4.19.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__combine__4_6_1",
        url = "https://crates.io/api/v1/crates/combine/4.6.1/download",
        type = "tar.gz",
        sha256 = "a909e4d93292cd8e9c42e189f61681eff9d67b6541f96b8a1a737f23737bd001",
        strip_prefix = "combine-4.6.1",
        build_file = Label("//cargo/remote:BUILD.combine-4.6.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__concurrent_queue__1_2_2",
        url = "https://crates.io/api/v1/crates/concurrent-queue/1.2.2/download",
        type = "tar.gz",
        sha256 = "30ed07550be01594c6026cff2a1d7fe9c8f683caa798e12b68694ac9e88286a3",
        strip_prefix = "concurrent-queue-1.2.2",
        build_file = Label("//cargo/remote:BUILD.concurrent-queue-1.2.2.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__config__0_11_0",
        url = "https://crates.io/api/v1/crates/config/0.11.0/download",
        type = "tar.gz",
        sha256 = "1b1b9d958c2b1368a663f05538fc1b5975adce1e19f435acceae987aceeeb369",
        strip_prefix = "config-0.11.0",
        build_file = Label("//cargo/remote:BUILD.config-0.11.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__core_foundation__0_9_1",
        url = "https://crates.io/api/v1/crates/core-foundation/0.9.1/download",
        type = "tar.gz",
        sha256 = "0a89e2ae426ea83155dccf10c0fa6b1463ef6d5fcb44cee0b224a408fa640a62",
        strip_prefix = "core-foundation-0.9.1",
        build_file = Label("//cargo/remote:BUILD.core-foundation-0.9.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__core_foundation_sys__0_8_2",
        url = "https://crates.io/api/v1/crates/core-foundation-sys/0.8.2/download",
        type = "tar.gz",
        sha256 = "ea221b5284a47e40033bf9b66f35f984ec0ea2931eb03505246cd27a963f981b",
        strip_prefix = "core-foundation-sys-0.8.2",
        build_file = Label("//cargo/remote:BUILD.core-foundation-sys-0.8.2.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__cpufeatures__0_2_1",
        url = "https://crates.io/api/v1/crates/cpufeatures/0.2.1/download",
        type = "tar.gz",
        sha256 = "95059428f66df56b63431fdb4e1947ed2190586af5c5a8a8b71122bdf5a7f469",
        strip_prefix = "cpufeatures-0.2.1",
        build_file = Label("//cargo/remote:BUILD.cpufeatures-0.2.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__crc16__0_4_0",
        url = "https://crates.io/api/v1/crates/crc16/0.4.0/download",
        type = "tar.gz",
        sha256 = "338089f42c427b86394a5ee60ff321da23a5c89c9d89514c829687b26359fcff",
        strip_prefix = "crc16-0.4.0",
        build_file = Label("//cargo/remote:BUILD.crc16-0.4.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__crc32fast__1_2_1",
        url = "https://crates.io/api/v1/crates/crc32fast/1.2.1/download",
        type = "tar.gz",
        sha256 = "81156fece84ab6a9f2afdb109ce3ae577e42b1228441eded99bd77f627953b1a",
        strip_prefix = "crc32fast-1.2.1",
        build_file = Label("//cargo/remote:BUILD.crc32fast-1.2.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__crossbeam_channel__0_5_1",
        url = "https://crates.io/api/v1/crates/crossbeam-channel/0.5.1/download",
        type = "tar.gz",
        sha256 = "06ed27e177f16d65f0f0c22a213e17c696ace5dd64b14258b52f9417ccb52db4",
        strip_prefix = "crossbeam-channel-0.5.1",
        build_file = Label("//cargo/remote:BUILD.crossbeam-channel-0.5.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__crossbeam_utils__0_8_5",
        url = "https://crates.io/api/v1/crates/crossbeam-utils/0.8.5/download",
        type = "tar.gz",
        sha256 = "d82cfc11ce7f2c3faef78d8a684447b40d503d9681acebed6cb728d45940c4db",
        strip_prefix = "crossbeam-utils-0.8.5",
        build_file = Label("//cargo/remote:BUILD.crossbeam-utils-0.8.5.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__crypto_mac__0_10_1",
        url = "https://crates.io/api/v1/crates/crypto-mac/0.10.1/download",
        type = "tar.gz",
        sha256 = "bff07008ec701e8028e2ceb8f83f0e4274ee62bd2dbdc4fefff2e9a91824081a",
        strip_prefix = "crypto-mac-0.10.1",
        build_file = Label("//cargo/remote:BUILD.crypto-mac-0.10.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__curve25519_dalek__3_2_0",
        url = "https://crates.io/api/v1/crates/curve25519-dalek/3.2.0/download",
        type = "tar.gz",
        sha256 = "0b9fdf9972b2bd6af2d913799d9ebc165ea4d2e65878e329d9c6b372c4491b61",
        strip_prefix = "curve25519-dalek-3.2.0",
        build_file = Label("//cargo/remote:BUILD.curve25519-dalek-3.2.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__data_encoding__2_3_2",
        url = "https://crates.io/api/v1/crates/data-encoding/2.3.2/download",
        type = "tar.gz",
        sha256 = "3ee2393c4a91429dffb4bedf19f4d6abf27d8a732c8ce4980305d782e5426d57",
        strip_prefix = "data-encoding-2.3.2",
        build_file = Label("//cargo/remote:BUILD.data-encoding-2.3.2.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__digest__0_9_0",
        url = "https://crates.io/api/v1/crates/digest/0.9.0/download",
        type = "tar.gz",
        sha256 = "d3dd60d1080a57a05ab032377049e0591415d2b31afd7028356dbf3cc6dcb066",
        strip_prefix = "digest-0.9.0",
        build_file = Label("//cargo/remote:BUILD.digest-0.9.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__dtoa__0_4_8",
        url = "https://crates.io/api/v1/crates/dtoa/0.4.8/download",
        type = "tar.gz",
        sha256 = "56899898ce76aaf4a0f24d914c97ea6ed976d42fec6ad33fcbb0a1103e07b2b0",
        strip_prefix = "dtoa-0.4.8",
        build_file = Label("//cargo/remote:BUILD.dtoa-0.4.8.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__ed25519__1_2_0",
        url = "https://crates.io/api/v1/crates/ed25519/1.2.0/download",
        type = "tar.gz",
        sha256 = "4620d40f6d2601794401d6dd95a5cf69b6c157852539470eeda433a99b3c0efc",
        strip_prefix = "ed25519-1.2.0",
        build_file = Label("//cargo/remote:BUILD.ed25519-1.2.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__ed25519_dalek__1_0_1",
        url = "https://crates.io/api/v1/crates/ed25519-dalek/1.0.1/download",
        type = "tar.gz",
        sha256 = "c762bae6dcaf24c4c84667b8579785430908723d5c889f469d76a41d59cc7a9d",
        strip_prefix = "ed25519-dalek-1.0.1",
        build_file = Label("//cargo/remote:BUILD.ed25519-dalek-1.0.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__enumflags2__0_7_1",
        url = "https://crates.io/api/v1/crates/enumflags2/0.7.1/download",
        type = "tar.gz",
        sha256 = "a8672257d642ffdd235f6e9c723c2326ac1253c8f3c022e7cfd2e57da55b1131",
        strip_prefix = "enumflags2-0.7.1",
        build_file = Label("//cargo/remote:BUILD.enumflags2-0.7.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__enumflags2_derive__0_7_0",
        url = "https://crates.io/api/v1/crates/enumflags2_derive/0.7.0/download",
        type = "tar.gz",
        sha256 = "33526f770a27828ce7c2792fdb7cb240220237e0ff12933ed6c23957fc5dd7cf",
        strip_prefix = "enumflags2_derive-0.7.0",
        build_file = Label("//cargo/remote:BUILD.enumflags2_derive-0.7.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__env_logger__0_7_1",
        url = "https://crates.io/api/v1/crates/env_logger/0.7.1/download",
        type = "tar.gz",
        sha256 = "44533bbbb3bb3c1fa17d9f2e4e38bbbaf8396ba82193c4cb1b6445d711445d36",
        strip_prefix = "env_logger-0.7.1",
        build_file = Label("//cargo/remote:BUILD.env_logger-0.7.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__event_listener__2_5_1",
        url = "https://crates.io/api/v1/crates/event-listener/2.5.1/download",
        type = "tar.gz",
        sha256 = "f7531096570974c3a9dcf9e4b8e1cede1ec26cf5046219fb3b9d897503b9be59",
        strip_prefix = "event-listener-2.5.1",
        build_file = Label("//cargo/remote:BUILD.event-listener-2.5.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__fastrand__1_5_0",
        url = "https://crates.io/api/v1/crates/fastrand/1.5.0/download",
        type = "tar.gz",
        sha256 = "b394ed3d285a429378d3b384b9eb1285267e7df4b166df24b7a6939a04dc392e",
        strip_prefix = "fastrand-1.5.0",
        build_file = Label("//cargo/remote:BUILD.fastrand-1.5.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__flate2__1_0_22",
        url = "https://crates.io/api/v1/crates/flate2/1.0.22/download",
        type = "tar.gz",
        sha256 = "1e6988e897c1c9c485f43b47a529cef42fde0547f9d8d41a7062518f1d8fc53f",
        strip_prefix = "flate2-1.0.22",
        build_file = Label("//cargo/remote:BUILD.flate2-1.0.22.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__fnv__1_0_7",
        url = "https://crates.io/api/v1/crates/fnv/1.0.7/download",
        type = "tar.gz",
        sha256 = "3f9eec918d3f24069decb9af1554cad7c880e2da24a9afd88aca000531ab82c1",
        strip_prefix = "fnv-1.0.7",
        build_file = Label("//cargo/remote:BUILD.fnv-1.0.7.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__foreign_types__0_3_2",
        url = "https://crates.io/api/v1/crates/foreign-types/0.3.2/download",
        type = "tar.gz",
        sha256 = "f6f339eb8adc052cd2ca78910fda869aefa38d22d5cb648e6485e4d3fc06f3b1",
        strip_prefix = "foreign-types-0.3.2",
        build_file = Label("//cargo/remote:BUILD.foreign-types-0.3.2.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__foreign_types_shared__0_1_1",
        url = "https://crates.io/api/v1/crates/foreign-types-shared/0.1.1/download",
        type = "tar.gz",
        sha256 = "00b0228411908ca8685dba7fc2cdd70ec9990a6e753e89b6ac91a84c40fbaf4b",
        strip_prefix = "foreign-types-shared-0.1.1",
        build_file = Label("//cargo/remote:BUILD.foreign-types-shared-0.1.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__form_urlencoded__1_0_1",
        url = "https://crates.io/api/v1/crates/form_urlencoded/1.0.1/download",
        type = "tar.gz",
        sha256 = "5fc25a87fa4fd2094bffb06925852034d90a17f0d1e05197d4956d3555752191",
        strip_prefix = "form_urlencoded-1.0.1",
        build_file = Label("//cargo/remote:BUILD.form_urlencoded-1.0.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__futures__0_3_17",
        url = "https://crates.io/api/v1/crates/futures/0.3.17/download",
        type = "tar.gz",
        sha256 = "a12aa0eb539080d55c3f2d45a67c3b58b6b0773c1a3ca2dfec66d58c97fd66ca",
        strip_prefix = "futures-0.3.17",
        build_file = Label("//cargo/remote:BUILD.futures-0.3.17.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__futures_channel__0_3_17",
        url = "https://crates.io/api/v1/crates/futures-channel/0.3.17/download",
        type = "tar.gz",
        sha256 = "5da6ba8c3bb3c165d3c7319fc1cc8304facf1fb8db99c5de877183c08a273888",
        strip_prefix = "futures-channel-0.3.17",
        build_file = Label("//cargo/remote:BUILD.futures-channel-0.3.17.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__futures_core__0_3_17",
        url = "https://crates.io/api/v1/crates/futures-core/0.3.17/download",
        type = "tar.gz",
        sha256 = "88d1c26957f23603395cd326b0ffe64124b818f4449552f960d815cfba83a53d",
        strip_prefix = "futures-core-0.3.17",
        build_file = Label("//cargo/remote:BUILD.futures-core-0.3.17.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__futures_executor__0_3_17",
        url = "https://crates.io/api/v1/crates/futures-executor/0.3.17/download",
        type = "tar.gz",
        sha256 = "45025be030969d763025784f7f355043dc6bc74093e4ecc5000ca4dc50d8745c",
        strip_prefix = "futures-executor-0.3.17",
        build_file = Label("//cargo/remote:BUILD.futures-executor-0.3.17.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__futures_io__0_3_17",
        url = "https://crates.io/api/v1/crates/futures-io/0.3.17/download",
        type = "tar.gz",
        sha256 = "522de2a0fe3e380f1bc577ba0474108faf3f6b18321dbf60b3b9c39a75073377",
        strip_prefix = "futures-io-0.3.17",
        build_file = Label("//cargo/remote:BUILD.futures-io-0.3.17.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__futures_lite__1_12_0",
        url = "https://crates.io/api/v1/crates/futures-lite/1.12.0/download",
        type = "tar.gz",
        sha256 = "7694489acd39452c77daa48516b894c153f192c3578d5a839b62c58099fcbf48",
        strip_prefix = "futures-lite-1.12.0",
        build_file = Label("//cargo/remote:BUILD.futures-lite-1.12.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__futures_macro__0_3_17",
        url = "https://crates.io/api/v1/crates/futures-macro/0.3.17/download",
        type = "tar.gz",
        sha256 = "18e4a4b95cea4b4ccbcf1c5675ca7c4ee4e9e75eb79944d07defde18068f79bb",
        strip_prefix = "futures-macro-0.3.17",
        build_file = Label("//cargo/remote:BUILD.futures-macro-0.3.17.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__futures_sink__0_3_17",
        url = "https://crates.io/api/v1/crates/futures-sink/0.3.17/download",
        type = "tar.gz",
        sha256 = "36ea153c13024fe480590b3e3d4cad89a0cfacecc24577b68f86c6ced9c2bc11",
        strip_prefix = "futures-sink-0.3.17",
        build_file = Label("//cargo/remote:BUILD.futures-sink-0.3.17.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__futures_task__0_3_17",
        url = "https://crates.io/api/v1/crates/futures-task/0.3.17/download",
        type = "tar.gz",
        sha256 = "1d3d00f4eddb73e498a54394f228cd55853bdf059259e8e7bc6e69d408892e99",
        strip_prefix = "futures-task-0.3.17",
        build_file = Label("//cargo/remote:BUILD.futures-task-0.3.17.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__futures_util__0_3_17",
        url = "https://crates.io/api/v1/crates/futures-util/0.3.17/download",
        type = "tar.gz",
        sha256 = "36568465210a3a6ee45e1f165136d68671471a501e632e9a98d96872222b5481",
        strip_prefix = "futures-util-0.3.17",
        build_file = Label("//cargo/remote:BUILD.futures-util-0.3.17.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__generic_array__0_14_4",
        url = "https://crates.io/api/v1/crates/generic-array/0.14.4/download",
        type = "tar.gz",
        sha256 = "501466ecc8a30d1d3b7fc9229b122b2ce8ed6e9d9223f1138d4babb253e51817",
        strip_prefix = "generic-array-0.14.4",
        build_file = Label("//cargo/remote:BUILD.generic-array-0.14.4.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__getrandom__0_1_16",
        url = "https://crates.io/api/v1/crates/getrandom/0.1.16/download",
        type = "tar.gz",
        sha256 = "8fc3cb4d91f53b50155bdcfd23f6a4c39ae1969c2ae85982b135750cccaf5fce",
        strip_prefix = "getrandom-0.1.16",
        build_file = Label("//cargo/remote:BUILD.getrandom-0.1.16.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__getrandom__0_2_3",
        url = "https://crates.io/api/v1/crates/getrandom/0.2.3/download",
        type = "tar.gz",
        sha256 = "7fcd999463524c52659517fe2cea98493cfe485d10565e7b0fb07dbba7ad2753",
        strip_prefix = "getrandom-0.2.3",
        build_file = Label("//cargo/remote:BUILD.getrandom-0.2.3.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__h2__0_3_4",
        url = "https://crates.io/api/v1/crates/h2/0.3.4/download",
        type = "tar.gz",
        sha256 = "d7f3675cfef6a30c8031cf9e6493ebdc3bb3272a3fea3923c4210d1830e6a472",
        strip_prefix = "h2-0.3.4",
        build_file = Label("//cargo/remote:BUILD.h2-0.3.4.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__hashbrown__0_11_2",
        url = "https://crates.io/api/v1/crates/hashbrown/0.11.2/download",
        type = "tar.gz",
        sha256 = "ab5ef0d4909ef3724cc8cce6ccc8572c5c817592e9285f5464f8e86f8bd3726e",
        strip_prefix = "hashbrown-0.11.2",
        build_file = Label("//cargo/remote:BUILD.hashbrown-0.11.2.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__hermit_abi__0_1_19",
        url = "https://crates.io/api/v1/crates/hermit-abi/0.1.19/download",
        type = "tar.gz",
        sha256 = "62b467343b94ba476dcb2500d242dadbb39557df889310ac77c5d99100aaac33",
        strip_prefix = "hermit-abi-0.1.19",
        build_file = Label("//cargo/remote:BUILD.hermit-abi-0.1.19.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__hex__0_4_3",
        url = "https://crates.io/api/v1/crates/hex/0.4.3/download",
        type = "tar.gz",
        sha256 = "7f24254aa9a54b5c858eaee2f5bccdb46aaf0e486a595ed5fd8f86ba55232a70",
        strip_prefix = "hex-0.4.3",
        build_file = Label("//cargo/remote:BUILD.hex-0.4.3.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__hmac__0_10_1",
        url = "https://crates.io/api/v1/crates/hmac/0.10.1/download",
        type = "tar.gz",
        sha256 = "c1441c6b1e930e2817404b5046f1f989899143a12bf92de603b69f4e0aee1e15",
        strip_prefix = "hmac-0.10.1",
        build_file = Label("//cargo/remote:BUILD.hmac-0.10.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__http__0_2_4",
        url = "https://crates.io/api/v1/crates/http/0.2.4/download",
        type = "tar.gz",
        sha256 = "527e8c9ac747e28542699a951517aa9a6945af506cd1f2e1b53a576c17b6cc11",
        strip_prefix = "http-0.2.4",
        build_file = Label("//cargo/remote:BUILD.http-0.2.4.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__http_body__0_4_3",
        url = "https://crates.io/api/v1/crates/http-body/0.4.3/download",
        type = "tar.gz",
        sha256 = "399c583b2979440c60be0821a6199eca73bc3c8dcd9d070d75ac726e2c6186e5",
        strip_prefix = "http-body-0.4.3",
        build_file = Label("//cargo/remote:BUILD.http-body-0.4.3.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__httparse__1_5_1",
        url = "https://crates.io/api/v1/crates/httparse/1.5.1/download",
        type = "tar.gz",
        sha256 = "acd94fdbe1d4ff688b67b04eee2e17bd50995534a61539e45adfefb45e5e5503",
        strip_prefix = "httparse-1.5.1",
        build_file = Label("//cargo/remote:BUILD.httparse-1.5.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__httpdate__1_0_1",
        url = "https://crates.io/api/v1/crates/httpdate/1.0.1/download",
        type = "tar.gz",
        sha256 = "6456b8a6c8f33fee7d958fcd1b60d55b11940a79e63ae87013e6d22e26034440",
        strip_prefix = "httpdate-1.0.1",
        build_file = Label("//cargo/remote:BUILD.httpdate-1.0.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__humantime__1_3_0",
        url = "https://crates.io/api/v1/crates/humantime/1.3.0/download",
        type = "tar.gz",
        sha256 = "df004cfca50ef23c36850aaaa59ad52cc70d0e90243c3c7737a4dd32dc7a3c4f",
        strip_prefix = "humantime-1.3.0",
        build_file = Label("//cargo/remote:BUILD.humantime-1.3.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__hyper__0_14_12",
        url = "https://crates.io/api/v1/crates/hyper/0.14.12/download",
        type = "tar.gz",
        sha256 = "13f67199e765030fa08fe0bd581af683f0d5bc04ea09c2b1102012c5fb90e7fd",
        strip_prefix = "hyper-0.14.12",
        build_file = Label("//cargo/remote:BUILD.hyper-0.14.12.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__hyper_tls__0_5_0",
        url = "https://crates.io/api/v1/crates/hyper-tls/0.5.0/download",
        type = "tar.gz",
        sha256 = "d6183ddfa99b85da61a140bea0efc93fdf56ceaa041b37d553518030827f9905",
        strip_prefix = "hyper-tls-0.5.0",
        build_file = Label("//cargo/remote:BUILD.hyper-tls-0.5.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__idna__0_2_3",
        url = "https://crates.io/api/v1/crates/idna/0.2.3/download",
        type = "tar.gz",
        sha256 = "418a0a6fab821475f634efe3ccc45c013f742efe03d853e8d3355d5cb850ecf8",
        strip_prefix = "idna-0.2.3",
        build_file = Label("//cargo/remote:BUILD.idna-0.2.3.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__indexmap__1_7_0",
        url = "https://crates.io/api/v1/crates/indexmap/1.7.0/download",
        type = "tar.gz",
        sha256 = "bc633605454125dec4b66843673f01c7df2b89479b32e0ed634e43a91cff62a5",
        strip_prefix = "indexmap-1.7.0",
        build_file = Label("//cargo/remote:BUILD.indexmap-1.7.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__instant__0_1_10",
        url = "https://crates.io/api/v1/crates/instant/0.1.10/download",
        type = "tar.gz",
        sha256 = "bee0328b1209d157ef001c94dd85b4f8f64139adb0eac2659f4b08382b2f474d",
        strip_prefix = "instant-0.1.10",
        build_file = Label("//cargo/remote:BUILD.instant-0.1.10.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__itoa__0_4_8",
        url = "https://crates.io/api/v1/crates/itoa/0.4.8/download",
        type = "tar.gz",
        sha256 = "b71991ff56294aa922b450139ee08b3bfc70982c6b2c7562771375cf73542dd4",
        strip_prefix = "itoa-0.4.8",
        build_file = Label("//cargo/remote:BUILD.itoa-0.4.8.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__js_sys__0_3_53",
        url = "https://crates.io/api/v1/crates/js-sys/0.3.53/download",
        type = "tar.gz",
        sha256 = "e4bf49d50e2961077d9c99f4b7997d770a1114f087c3c2e0069b36c13fc2979d",
        strip_prefix = "js-sys-0.3.53",
        build_file = Label("//cargo/remote:BUILD.js-sys-0.3.53.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__json__0_12_4",
        url = "https://crates.io/api/v1/crates/json/0.12.4/download",
        type = "tar.gz",
        sha256 = "078e285eafdfb6c4b434e0d31e8cfcb5115b651496faca5749b88fafd4f23bfd",
        strip_prefix = "json-0.12.4",
        build_file = Label("//cargo/remote:BUILD.json-0.12.4.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__lazy_static__1_4_0",
        url = "https://crates.io/api/v1/crates/lazy_static/1.4.0/download",
        type = "tar.gz",
        sha256 = "e2abad23fbc42b3700f2f279844dc832adb2b2eb069b2df918f455c4e18cc646",
        strip_prefix = "lazy_static-1.4.0",
        build_file = Label("//cargo/remote:BUILD.lazy_static-1.4.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__lexical_core__0_7_6",
        url = "https://crates.io/api/v1/crates/lexical-core/0.7.6/download",
        type = "tar.gz",
        sha256 = "6607c62aa161d23d17a9072cc5da0be67cdfc89d3afb1e8d9c842bebc2525ffe",
        strip_prefix = "lexical-core-0.7.6",
        build_file = Label("//cargo/remote:BUILD.lexical-core-0.7.6.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__libc__0_2_101",
        url = "https://crates.io/api/v1/crates/libc/0.2.101/download",
        type = "tar.gz",
        sha256 = "3cb00336871be5ed2c8ed44b60ae9959dc5b9f08539422ed43f09e34ecaeba21",
        strip_prefix = "libc-0.2.101",
        build_file = Label("//cargo/remote:BUILD.libc-0.2.101.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__linked_hash_map__0_5_4",
        url = "https://crates.io/api/v1/crates/linked-hash-map/0.5.4/download",
        type = "tar.gz",
        sha256 = "7fb9b38af92608140b86b693604b9ffcc5824240a484d1ecd4795bacb2fe88f3",
        strip_prefix = "linked-hash-map-0.5.4",
        build_file = Label("//cargo/remote:BUILD.linked-hash-map-0.5.4.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__lock_api__0_4_5",
        url = "https://crates.io/api/v1/crates/lock_api/0.4.5/download",
        type = "tar.gz",
        sha256 = "712a4d093c9976e24e7dbca41db895dabcbac38eb5f4045393d17a95bdfb1109",
        strip_prefix = "lock_api-0.4.5",
        build_file = Label("//cargo/remote:BUILD.lock_api-0.4.5.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__log__0_4_14",
        url = "https://crates.io/api/v1/crates/log/0.4.14/download",
        type = "tar.gz",
        sha256 = "51b9bbe6c47d51fc3e1a9b945965946b4c44142ab8792c50835a980d362c2710",
        strip_prefix = "log-0.4.14",
        build_file = Label("//cargo/remote:BUILD.log-0.4.14.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__matches__0_1_9",
        url = "https://crates.io/api/v1/crates/matches/0.1.9/download",
        type = "tar.gz",
        sha256 = "a3e378b66a060d48947b590737b30a1be76706c8dd7b8ba0f2fe3989c68a853f",
        strip_prefix = "matches-0.1.9",
        build_file = Label("//cargo/remote:BUILD.matches-0.1.9.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__memchr__2_4_1",
        url = "https://crates.io/api/v1/crates/memchr/2.4.1/download",
        type = "tar.gz",
        sha256 = "308cc39be01b73d0d18f82a0e7b2a3df85245f84af96fdddc5d202d27e47b86a",
        strip_prefix = "memchr-2.4.1",
        build_file = Label("//cargo/remote:BUILD.memchr-2.4.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__miniz_oxide__0_4_4",
        url = "https://crates.io/api/v1/crates/miniz_oxide/0.4.4/download",
        type = "tar.gz",
        sha256 = "a92518e98c078586bc6c934028adcca4c92a53d6a958196de835170a01d84e4b",
        strip_prefix = "miniz_oxide-0.4.4",
        build_file = Label("//cargo/remote:BUILD.miniz_oxide-0.4.4.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__mio__0_7_13",
        url = "https://crates.io/api/v1/crates/mio/0.7.13/download",
        type = "tar.gz",
        sha256 = "8c2bdb6314ec10835cd3293dd268473a835c02b7b352e788be788b3c6ca6bb16",
        strip_prefix = "mio-0.7.13",
        build_file = Label("//cargo/remote:BUILD.mio-0.7.13.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__miow__0_3_7",
        url = "https://crates.io/api/v1/crates/miow/0.3.7/download",
        type = "tar.gz",
        sha256 = "b9f1c5b025cda876f66ef43a113f91ebc9f4ccef34843000e0adf6ebbab84e21",
        strip_prefix = "miow-0.3.7",
        build_file = Label("//cargo/remote:BUILD.miow-0.3.7.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__native_tls__0_2_8",
        url = "https://crates.io/api/v1/crates/native-tls/0.2.8/download",
        type = "tar.gz",
        sha256 = "48ba9f7719b5a0f42f338907614285fb5fd70e53858141f69898a1fb7203b24d",
        strip_prefix = "native-tls-0.2.8",
        build_file = Label("//cargo/remote:BUILD.native-tls-0.2.8.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__nats__0_15_2",
        url = "https://crates.io/api/v1/crates/nats/0.15.2/download",
        type = "tar.gz",
        sha256 = "2a3097b182107db2cf690280d61f23f17ee31d49f3994ad152ee6a10261f77c3",
        strip_prefix = "nats-0.15.2",
        build_file = Label("//cargo/remote:BUILD.nats-0.15.2.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__nkeys__0_1_0",
        url = "https://crates.io/api/v1/crates/nkeys/0.1.0/download",
        type = "tar.gz",
        sha256 = "c1a98f0a974ff737974b57ba1c71d2e0fe7ec18e5a828d4b8e02683171349dfa",
        strip_prefix = "nkeys-0.1.0",
        build_file = Label("//cargo/remote:BUILD.nkeys-0.1.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__nom__5_1_2",
        url = "https://crates.io/api/v1/crates/nom/5.1.2/download",
        type = "tar.gz",
        sha256 = "ffb4262d26ed83a1c0a33a38fe2bb15797329c85770da05e6b828ddb782627af",
        strip_prefix = "nom-5.1.2",
        build_file = Label("//cargo/remote:BUILD.nom-5.1.2.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__ntapi__0_3_6",
        url = "https://crates.io/api/v1/crates/ntapi/0.3.6/download",
        type = "tar.gz",
        sha256 = "3f6bb902e437b6d86e03cce10a7e2af662292c5dfef23b65899ea3ac9354ad44",
        strip_prefix = "ntapi-0.3.6",
        build_file = Label("//cargo/remote:BUILD.ntapi-0.3.6.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__nuid__0_3_0",
        url = "https://crates.io/api/v1/crates/nuid/0.3.0/download",
        type = "tar.gz",
        sha256 = "7000c9392b545c4ba43e8abc086bf7d01cd2948690934c16980170b0549a2bd3",
        strip_prefix = "nuid-0.3.0",
        build_file = Label("//cargo/remote:BUILD.nuid-0.3.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__num__0_4_0",
        url = "https://crates.io/api/v1/crates/num/0.4.0/download",
        type = "tar.gz",
        sha256 = "43db66d1170d347f9a065114077f7dccb00c1b9478c89384490a3425279a4606",
        strip_prefix = "num-0.4.0",
        build_file = Label("//cargo/remote:BUILD.num-0.4.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__num_bigint__0_4_2",
        url = "https://crates.io/api/v1/crates/num-bigint/0.4.2/download",
        type = "tar.gz",
        sha256 = "74e768dff5fb39a41b3bcd30bb25cf989706c90d028d1ad71971987aa309d535",
        strip_prefix = "num-bigint-0.4.2",
        build_file = Label("//cargo/remote:BUILD.num-bigint-0.4.2.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__num_complex__0_4_0",
        url = "https://crates.io/api/v1/crates/num-complex/0.4.0/download",
        type = "tar.gz",
        sha256 = "26873667bbbb7c5182d4a37c1add32cdf09f841af72da53318fdb81543c15085",
        strip_prefix = "num-complex-0.4.0",
        build_file = Label("//cargo/remote:BUILD.num-complex-0.4.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__num_derive__0_3_3",
        url = "https://crates.io/api/v1/crates/num-derive/0.3.3/download",
        type = "tar.gz",
        sha256 = "876a53fff98e03a936a674b29568b0e605f06b29372c2489ff4de23f1949743d",
        strip_prefix = "num-derive-0.3.3",
        build_file = Label("//cargo/remote:BUILD.num-derive-0.3.3.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__num_integer__0_1_44",
        url = "https://crates.io/api/v1/crates/num-integer/0.1.44/download",
        type = "tar.gz",
        sha256 = "d2cc698a63b549a70bc047073d2949cce27cd1c7b0a4a862d08a8031bc2801db",
        strip_prefix = "num-integer-0.1.44",
        build_file = Label("//cargo/remote:BUILD.num-integer-0.1.44.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__num_iter__0_1_42",
        url = "https://crates.io/api/v1/crates/num-iter/0.1.42/download",
        type = "tar.gz",
        sha256 = "b2021c8337a54d21aca0d59a92577a029af9431cb59b909b03252b9c164fad59",
        strip_prefix = "num-iter-0.1.42",
        build_file = Label("//cargo/remote:BUILD.num-iter-0.1.42.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__num_rational__0_4_0",
        url = "https://crates.io/api/v1/crates/num-rational/0.4.0/download",
        type = "tar.gz",
        sha256 = "d41702bd167c2df5520b384281bc111a4b5efcf7fbc4c9c222c815b07e0a6a6a",
        strip_prefix = "num-rational-0.4.0",
        build_file = Label("//cargo/remote:BUILD.num-rational-0.4.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__num_traits__0_1_43",
        url = "https://crates.io/api/v1/crates/num-traits/0.1.43/download",
        type = "tar.gz",
        sha256 = "92e5113e9fd4cc14ded8e499429f396a20f98c772a47cc8622a736e1ec843c31",
        strip_prefix = "num-traits-0.1.43",
        build_file = Label("//cargo/remote:BUILD.num-traits-0.1.43.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__num_traits__0_2_14",
        url = "https://crates.io/api/v1/crates/num-traits/0.2.14/download",
        type = "tar.gz",
        sha256 = "9a64b1ec5cda2586e284722486d802acf1f7dbdc623e2bfc57e65ca1cd099290",
        strip_prefix = "num-traits-0.2.14",
        build_file = Label("//cargo/remote:BUILD.num-traits-0.2.14.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__num_cpus__1_13_0",
        url = "https://crates.io/api/v1/crates/num_cpus/1.13.0/download",
        type = "tar.gz",
        sha256 = "05499f3756671c15885fee9034446956fff3f243d6077b91e5767df161f766b3",
        strip_prefix = "num_cpus-1.13.0",
        build_file = Label("//cargo/remote:BUILD.num_cpus-1.13.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__once_cell__1_8_0",
        url = "https://crates.io/api/v1/crates/once_cell/1.8.0/download",
        type = "tar.gz",
        sha256 = "692fcb63b64b1758029e0a96ee63e049ce8c5948587f2f7208df04625e5f6b56",
        strip_prefix = "once_cell-1.8.0",
        build_file = Label("//cargo/remote:BUILD.once_cell-1.8.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__opaque_debug__0_3_0",
        url = "https://crates.io/api/v1/crates/opaque-debug/0.3.0/download",
        type = "tar.gz",
        sha256 = "624a8340c38c1b80fd549087862da4ba43e08858af025b236e509b6649fc13d5",
        strip_prefix = "opaque-debug-0.3.0",
        build_file = Label("//cargo/remote:BUILD.opaque-debug-0.3.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__openssl__0_10_36",
        url = "https://crates.io/api/v1/crates/openssl/0.10.36/download",
        type = "tar.gz",
        sha256 = "8d9facdb76fec0b73c406f125d44d86fdad818d66fef0531eec9233ca425ff4a",
        strip_prefix = "openssl-0.10.36",
        build_file = Label("//cargo/remote:BUILD.openssl-0.10.36.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__openssl_probe__0_1_4",
        url = "https://crates.io/api/v1/crates/openssl-probe/0.1.4/download",
        type = "tar.gz",
        sha256 = "28988d872ab76095a6e6ac88d99b54fd267702734fd7ffe610ca27f533ddb95a",
        strip_prefix = "openssl-probe-0.1.4",
        build_file = Label("//cargo/remote:BUILD.openssl-probe-0.1.4.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__openssl_sys__0_9_67",
        url = "https://crates.io/api/v1/crates/openssl-sys/0.9.67/download",
        type = "tar.gz",
        sha256 = "69df2d8dfc6ce3aaf44b40dec6f487d5a886516cf6879c49e98e0710f310a058",
        strip_prefix = "openssl-sys-0.9.67",
        build_file = Label("//cargo/remote:BUILD.openssl-sys-0.9.67.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__parking__2_0_0",
        url = "https://crates.io/api/v1/crates/parking/2.0.0/download",
        type = "tar.gz",
        sha256 = "427c3892f9e783d91cc128285287e70a59e206ca452770ece88a76f7a3eddd72",
        strip_prefix = "parking-2.0.0",
        build_file = Label("//cargo/remote:BUILD.parking-2.0.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__parking_lot__0_11_2",
        url = "https://crates.io/api/v1/crates/parking_lot/0.11.2/download",
        type = "tar.gz",
        sha256 = "7d17b78036a60663b797adeaee46f5c9dfebb86948d1255007a1d6be0271ff99",
        strip_prefix = "parking_lot-0.11.2",
        build_file = Label("//cargo/remote:BUILD.parking_lot-0.11.2.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__parking_lot_core__0_8_5",
        url = "https://crates.io/api/v1/crates/parking_lot_core/0.8.5/download",
        type = "tar.gz",
        sha256 = "d76e8e1493bcac0d2766c42737f34458f1c8c50c0d23bcb24ea953affb273216",
        strip_prefix = "parking_lot_core-0.8.5",
        build_file = Label("//cargo/remote:BUILD.parking_lot_core-0.8.5.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__percent_encoding__2_1_0",
        url = "https://crates.io/api/v1/crates/percent-encoding/2.1.0/download",
        type = "tar.gz",
        sha256 = "d4fd5641d01c8f18a23da7b6fe29298ff4b55afcccdf78973b24cf3175fee32e",
        strip_prefix = "percent-encoding-2.1.0",
        build_file = Label("//cargo/remote:BUILD.percent-encoding-2.1.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__pin_project__1_0_8",
        url = "https://crates.io/api/v1/crates/pin-project/1.0.8/download",
        type = "tar.gz",
        sha256 = "576bc800220cc65dac09e99e97b08b358cfab6e17078de8dc5fee223bd2d0c08",
        strip_prefix = "pin-project-1.0.8",
        build_file = Label("//cargo/remote:BUILD.pin-project-1.0.8.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__pin_project_internal__1_0_8",
        url = "https://crates.io/api/v1/crates/pin-project-internal/1.0.8/download",
        type = "tar.gz",
        sha256 = "6e8fe8163d14ce7f0cdac2e040116f22eac817edabff0be91e8aff7e9accf389",
        strip_prefix = "pin-project-internal-1.0.8",
        build_file = Label("//cargo/remote:BUILD.pin-project-internal-1.0.8.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__pin_project_lite__0_2_7",
        url = "https://crates.io/api/v1/crates/pin-project-lite/0.2.7/download",
        type = "tar.gz",
        sha256 = "8d31d11c69a6b52a174b42bdc0c30e5e11670f90788b2c471c31c1d17d449443",
        strip_prefix = "pin-project-lite-0.2.7",
        build_file = Label("//cargo/remote:BUILD.pin-project-lite-0.2.7.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__pin_utils__0_1_0",
        url = "https://crates.io/api/v1/crates/pin-utils/0.1.0/download",
        type = "tar.gz",
        sha256 = "8b870d8c151b6f2fb93e84a13146138f05d02ed11c7e7c54f8826aaaf7c9f184",
        strip_prefix = "pin-utils-0.1.0",
        build_file = Label("//cargo/remote:BUILD.pin-utils-0.1.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__pkg_config__0_3_19",
        url = "https://crates.io/api/v1/crates/pkg-config/0.3.19/download",
        type = "tar.gz",
        sha256 = "3831453b3449ceb48b6d9c7ad7c96d5ea673e9b470a1dc578c2ce6521230884c",
        strip_prefix = "pkg-config-0.3.19",
        build_file = Label("//cargo/remote:BUILD.pkg-config-0.3.19.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__ppv_lite86__0_2_10",
        url = "https://crates.io/api/v1/crates/ppv-lite86/0.2.10/download",
        type = "tar.gz",
        sha256 = "ac74c624d6b2d21f425f752262f42188365d7b8ff1aff74c82e45136510a4857",
        strip_prefix = "ppv-lite86-0.2.10",
        build_file = Label("//cargo/remote:BUILD.ppv-lite86-0.2.10.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__pretty_env_logger__0_4_0",
        url = "https://crates.io/api/v1/crates/pretty_env_logger/0.4.0/download",
        type = "tar.gz",
        sha256 = "926d36b9553851b8b0005f1275891b392ee4d2d833852c417ed025477350fb9d",
        strip_prefix = "pretty_env_logger-0.4.0",
        build_file = Label("//cargo/remote:BUILD.pretty_env_logger-0.4.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__proc_macro_hack__0_5_19",
        url = "https://crates.io/api/v1/crates/proc-macro-hack/0.5.19/download",
        type = "tar.gz",
        sha256 = "dbf0c48bc1d91375ae5c3cd81e3722dff1abcf81a30960240640d223f59fe0e5",
        strip_prefix = "proc-macro-hack-0.5.19",
        build_file = Label("//cargo/remote:BUILD.proc-macro-hack-0.5.19.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__proc_macro_nested__0_1_7",
        url = "https://crates.io/api/v1/crates/proc-macro-nested/0.1.7/download",
        type = "tar.gz",
        sha256 = "bc881b2c22681370c6a780e47af9840ef841837bc98118431d4e1868bd0c1086",
        strip_prefix = "proc-macro-nested-0.1.7",
        build_file = Label("//cargo/remote:BUILD.proc-macro-nested-0.1.7.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__proc_macro2__1_0_29",
        url = "https://crates.io/api/v1/crates/proc-macro2/1.0.29/download",
        type = "tar.gz",
        sha256 = "b9f5105d4fdaab20335ca9565e106a5d9b82b6219b5ba735731124ac6711d23d",
        strip_prefix = "proc-macro2-1.0.29",
        build_file = Label("//cargo/remote:BUILD.proc-macro2-1.0.29.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__procfs__0_9_1",
        url = "https://crates.io/api/v1/crates/procfs/0.9.1/download",
        type = "tar.gz",
        sha256 = "ab8809e0c18450a2db0f236d2a44ec0b4c1412d0eb936233579f0990faa5d5cd",
        strip_prefix = "procfs-0.9.1",
        build_file = Label("//cargo/remote:BUILD.procfs-0.9.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__prometheus__0_12_0",
        url = "https://crates.io/api/v1/crates/prometheus/0.12.0/download",
        type = "tar.gz",
        sha256 = "5986aa8d62380092d2f50f8b1cdba9cb9b6731ffd4b25b51fd126b6c3e05b99c",
        strip_prefix = "prometheus-0.12.0",
        build_file = Label("//cargo/remote:BUILD.prometheus-0.12.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__protobuf__2_25_1",
        url = "https://crates.io/api/v1/crates/protobuf/2.25.1/download",
        type = "tar.gz",
        sha256 = "23129d50f2c9355ced935fce8a08bd706ee2e7ce2b3b33bf61dace0e379ac63a",
        strip_prefix = "protobuf-2.25.1",
        build_file = Label("//cargo/remote:BUILD.protobuf-2.25.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__quick_error__1_2_3",
        url = "https://crates.io/api/v1/crates/quick-error/1.2.3/download",
        type = "tar.gz",
        sha256 = "a1d01941d82fa2ab50be1e79e6714289dd7cde78eba4c074bc5a4374f650dfe0",
        strip_prefix = "quick-error-1.2.3",
        build_file = Label("//cargo/remote:BUILD.quick-error-1.2.3.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__quote__1_0_9",
        url = "https://crates.io/api/v1/crates/quote/1.0.9/download",
        type = "tar.gz",
        sha256 = "c3d0b9745dc2debf507c8422de05d7226cc1f0644216dfdfead988f9b1ab32a7",
        strip_prefix = "quote-1.0.9",
        build_file = Label("//cargo/remote:BUILD.quote-1.0.9.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__rand__0_7_3",
        url = "https://crates.io/api/v1/crates/rand/0.7.3/download",
        type = "tar.gz",
        sha256 = "6a6b1679d49b24bbfe0c803429aa1874472f50d9b363131f0e89fc356b544d03",
        strip_prefix = "rand-0.7.3",
        build_file = Label("//cargo/remote:BUILD.rand-0.7.3.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__rand__0_8_4",
        url = "https://crates.io/api/v1/crates/rand/0.8.4/download",
        type = "tar.gz",
        sha256 = "2e7573632e6454cf6b99d7aac4ccca54be06da05aca2ef7423d22d27d4d4bcd8",
        strip_prefix = "rand-0.8.4",
        build_file = Label("//cargo/remote:BUILD.rand-0.8.4.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__rand_chacha__0_2_2",
        url = "https://crates.io/api/v1/crates/rand_chacha/0.2.2/download",
        type = "tar.gz",
        sha256 = "f4c8ed856279c9737206bf725bf36935d8666ead7aa69b52be55af369d193402",
        strip_prefix = "rand_chacha-0.2.2",
        build_file = Label("//cargo/remote:BUILD.rand_chacha-0.2.2.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__rand_chacha__0_3_1",
        url = "https://crates.io/api/v1/crates/rand_chacha/0.3.1/download",
        type = "tar.gz",
        sha256 = "e6c10a63a0fa32252be49d21e7709d4d4baf8d231c2dbce1eaa8141b9b127d88",
        strip_prefix = "rand_chacha-0.3.1",
        build_file = Label("//cargo/remote:BUILD.rand_chacha-0.3.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__rand_core__0_5_1",
        url = "https://crates.io/api/v1/crates/rand_core/0.5.1/download",
        type = "tar.gz",
        sha256 = "90bde5296fc891b0cef12a6d03ddccc162ce7b2aff54160af9338f8d40df6d19",
        strip_prefix = "rand_core-0.5.1",
        build_file = Label("//cargo/remote:BUILD.rand_core-0.5.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__rand_core__0_6_3",
        url = "https://crates.io/api/v1/crates/rand_core/0.6.3/download",
        type = "tar.gz",
        sha256 = "d34f1408f55294453790c48b2f1ebbb1c5b4b7563eb1f418bcfcfdbb06ebb4e7",
        strip_prefix = "rand_core-0.6.3",
        build_file = Label("//cargo/remote:BUILD.rand_core-0.6.3.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__rand_hc__0_2_0",
        url = "https://crates.io/api/v1/crates/rand_hc/0.2.0/download",
        type = "tar.gz",
        sha256 = "ca3129af7b92a17112d59ad498c6f81eaf463253766b90396d39ea7a39d6613c",
        strip_prefix = "rand_hc-0.2.0",
        build_file = Label("//cargo/remote:BUILD.rand_hc-0.2.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__rand_hc__0_3_1",
        url = "https://crates.io/api/v1/crates/rand_hc/0.3.1/download",
        type = "tar.gz",
        sha256 = "d51e9f596de227fda2ea6c84607f5558e196eeaf43c986b724ba4fb8fdf497e7",
        strip_prefix = "rand_hc-0.3.1",
        build_file = Label("//cargo/remote:BUILD.rand_hc-0.3.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__redis__0_21_2",
        url = "https://crates.io/api/v1/crates/redis/0.21.2/download",
        type = "tar.gz",
        sha256 = "202c5bf92cad3d57605c366e644a7fbf305a83f19754fc66678c6265dcc9b8b4",
        strip_prefix = "redis-0.21.2",
        build_file = Label("//cargo/remote:BUILD.redis-0.21.2.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__redox_syscall__0_2_10",
        url = "https://crates.io/api/v1/crates/redox_syscall/0.2.10/download",
        type = "tar.gz",
        sha256 = "8383f39639269cde97d255a32bdb68c047337295414940c68bdd30c2e13203ff",
        strip_prefix = "redox_syscall-0.2.10",
        build_file = Label("//cargo/remote:BUILD.redox_syscall-0.2.10.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__regex__1_5_4",
        url = "https://crates.io/api/v1/crates/regex/1.5.4/download",
        type = "tar.gz",
        sha256 = "d07a8629359eb56f1e2fb1652bb04212c072a87ba68546a04065d525673ac461",
        strip_prefix = "regex-1.5.4",
        build_file = Label("//cargo/remote:BUILD.regex-1.5.4.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__regex_syntax__0_6_25",
        url = "https://crates.io/api/v1/crates/regex-syntax/0.6.25/download",
        type = "tar.gz",
        sha256 = "f497285884f3fcff424ffc933e56d7cbca511def0c9831a7f9b5f6153e3cc89b",
        strip_prefix = "regex-syntax-0.6.25",
        build_file = Label("//cargo/remote:BUILD.regex-syntax-0.6.25.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__remove_dir_all__0_5_3",
        url = "https://crates.io/api/v1/crates/remove_dir_all/0.5.3/download",
        type = "tar.gz",
        sha256 = "3acd125665422973a33ac9d3dd2df85edad0f4ae9b00dafb1a05e43a9f5ef8e7",
        strip_prefix = "remove_dir_all-0.5.3",
        build_file = Label("//cargo/remote:BUILD.remove_dir_all-0.5.3.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__ring__0_16_20",
        url = "https://crates.io/api/v1/crates/ring/0.16.20/download",
        type = "tar.gz",
        sha256 = "3053cf52e236a3ed746dfc745aa9cacf1b791d846bdaf412f60a8d7d6e17c8fc",
        strip_prefix = "ring-0.16.20",
        build_file = Label("//cargo/remote:BUILD.ring-0.16.20.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__rust_ini__0_13_0",
        url = "https://crates.io/api/v1/crates/rust-ini/0.13.0/download",
        type = "tar.gz",
        sha256 = "3e52c148ef37f8c375d49d5a73aa70713125b7f19095948a923f80afdeb22ec2",
        strip_prefix = "rust-ini-0.13.0",
        build_file = Label("//cargo/remote:BUILD.rust-ini-0.13.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__rustls__0_19_1",
        url = "https://crates.io/api/v1/crates/rustls/0.19.1/download",
        type = "tar.gz",
        sha256 = "35edb675feee39aec9c99fa5ff985081995a06d594114ae14cbe797ad7b7a6d7",
        strip_prefix = "rustls-0.19.1",
        build_file = Label("//cargo/remote:BUILD.rustls-0.19.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__rustls_native_certs__0_5_0",
        url = "https://crates.io/api/v1/crates/rustls-native-certs/0.5.0/download",
        type = "tar.gz",
        sha256 = "5a07b7c1885bd8ed3831c289b7870b13ef46fe0e856d288c30d9cc17d75a2092",
        strip_prefix = "rustls-native-certs-0.5.0",
        build_file = Label("//cargo/remote:BUILD.rustls-native-certs-0.5.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__ryu__1_0_5",
        url = "https://crates.io/api/v1/crates/ryu/1.0.5/download",
        type = "tar.gz",
        sha256 = "71d301d4193d031abdd79ff7e3dd721168a9572ef3fe51a1517aba235bd8f86e",
        strip_prefix = "ryu-1.0.5",
        build_file = Label("//cargo/remote:BUILD.ryu-1.0.5.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__schannel__0_1_19",
        url = "https://crates.io/api/v1/crates/schannel/0.1.19/download",
        type = "tar.gz",
        sha256 = "8f05ba609c234e60bee0d547fe94a4c7e9da733d1c962cf6e59efa4cd9c8bc75",
        strip_prefix = "schannel-0.1.19",
        build_file = Label("//cargo/remote:BUILD.schannel-0.1.19.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__scopeguard__1_1_0",
        url = "https://crates.io/api/v1/crates/scopeguard/1.1.0/download",
        type = "tar.gz",
        sha256 = "d29ab0c6d3fc0ee92fe66e2d99f700eab17a8d57d1c1d3b748380fb20baa78cd",
        strip_prefix = "scopeguard-1.1.0",
        build_file = Label("//cargo/remote:BUILD.scopeguard-1.1.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__sct__0_6_1",
        url = "https://crates.io/api/v1/crates/sct/0.6.1/download",
        type = "tar.gz",
        sha256 = "b362b83898e0e69f38515b82ee15aa80636befe47c3b6d3d89a911e78fc228ce",
        strip_prefix = "sct-0.6.1",
        build_file = Label("//cargo/remote:BUILD.sct-0.6.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__security_framework__2_4_2",
        url = "https://crates.io/api/v1/crates/security-framework/2.4.2/download",
        type = "tar.gz",
        sha256 = "525bc1abfda2e1998d152c45cf13e696f76d0a4972310b22fac1658b05df7c87",
        strip_prefix = "security-framework-2.4.2",
        build_file = Label("//cargo/remote:BUILD.security-framework-2.4.2.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__security_framework_sys__2_4_2",
        url = "https://crates.io/api/v1/crates/security-framework-sys/2.4.2/download",
        type = "tar.gz",
        sha256 = "a9dd14d83160b528b7bfd66439110573efcfbe281b17fc2ca9f39f550d619c7e",
        strip_prefix = "security-framework-sys-2.4.2",
        build_file = Label("//cargo/remote:BUILD.security-framework-sys-2.4.2.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__serde__0_8_23",
        url = "https://crates.io/api/v1/crates/serde/0.8.23/download",
        type = "tar.gz",
        sha256 = "9dad3f759919b92c3068c696c15c3d17238234498bbdcc80f2c469606f948ac8",
        strip_prefix = "serde-0.8.23",
        build_file = Label("//cargo/remote:BUILD.serde-0.8.23.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__serde__1_0_130",
        url = "https://crates.io/api/v1/crates/serde/1.0.130/download",
        type = "tar.gz",
        sha256 = "f12d06de37cf59146fbdecab66aa99f9fe4f78722e3607577a5375d66bd0c913",
        strip_prefix = "serde-1.0.130",
        build_file = Label("//cargo/remote:BUILD.serde-1.0.130.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__serde_hjson__0_9_1",
        url = "https://crates.io/api/v1/crates/serde-hjson/0.9.1/download",
        type = "tar.gz",
        sha256 = "6a3a4e0ea8a88553209f6cc6cfe8724ecad22e1acf372793c27d995290fe74f8",
        strip_prefix = "serde-hjson-0.9.1",
        build_file = Label("//cargo/remote:BUILD.serde-hjson-0.9.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__serde_derive__1_0_130",
        url = "https://crates.io/api/v1/crates/serde_derive/1.0.130/download",
        type = "tar.gz",
        sha256 = "d7bc1a1ab1961464eae040d96713baa5a724a8152c1222492465b54322ec508b",
        strip_prefix = "serde_derive-1.0.130",
        build_file = Label("//cargo/remote:BUILD.serde_derive-1.0.130.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__serde_json__1_0_67",
        url = "https://crates.io/api/v1/crates/serde_json/1.0.67/download",
        type = "tar.gz",
        sha256 = "a7f9e390c27c3c0ce8bc5d725f6e4d30a29d26659494aa4b17535f7522c5c950",
        strip_prefix = "serde_json-1.0.67",
        build_file = Label("//cargo/remote:BUILD.serde_json-1.0.67.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__serde_repr__0_1_7",
        url = "https://crates.io/api/v1/crates/serde_repr/0.1.7/download",
        type = "tar.gz",
        sha256 = "98d0516900518c29efa217c298fa1f4e6c6ffc85ae29fd7f4ee48f176e1a9ed5",
        strip_prefix = "serde_repr-0.1.7",
        build_file = Label("//cargo/remote:BUILD.serde_repr-0.1.7.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__sha_1__0_9_8",
        url = "https://crates.io/api/v1/crates/sha-1/0.9.8/download",
        type = "tar.gz",
        sha256 = "99cd6713db3cf16b6c84e06321e049a9b9f699826e16096d23bbcc44d15d51a6",
        strip_prefix = "sha-1-0.9.8",
        build_file = Label("//cargo/remote:BUILD.sha-1-0.9.8.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__sha1__0_6_0",
        url = "https://crates.io/api/v1/crates/sha1/0.6.0/download",
        type = "tar.gz",
        sha256 = "2579985fda508104f7587689507983eadd6a6e84dd35d6d115361f530916fa0d",
        strip_prefix = "sha1-0.6.0",
        build_file = Label("//cargo/remote:BUILD.sha1-0.6.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__sha2__0_9_6",
        url = "https://crates.io/api/v1/crates/sha2/0.9.6/download",
        type = "tar.gz",
        sha256 = "9204c41a1597a8c5af23c82d1c921cb01ec0a4c59e07a9c7306062829a3903f3",
        strip_prefix = "sha2-0.9.6",
        build_file = Label("//cargo/remote:BUILD.sha2-0.9.6.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__signal_hook_registry__1_4_0",
        url = "https://crates.io/api/v1/crates/signal-hook-registry/1.4.0/download",
        type = "tar.gz",
        sha256 = "e51e73328dc4ac0c7ccbda3a494dfa03df1de2f46018127f60c693f2648455b0",
        strip_prefix = "signal-hook-registry-1.4.0",
        build_file = Label("//cargo/remote:BUILD.signal-hook-registry-1.4.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__signatory__0_21_0",
        url = "https://crates.io/api/v1/crates/signatory/0.21.0/download",
        type = "tar.gz",
        sha256 = "9eaebd4be561a7d8148803baa108092f85090189c4b8c3ffb81602b15b5c1771",
        strip_prefix = "signatory-0.21.0",
        build_file = Label("//cargo/remote:BUILD.signatory-0.21.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__signature__1_3_1",
        url = "https://crates.io/api/v1/crates/signature/1.3.1/download",
        type = "tar.gz",
        sha256 = "c19772be3c4dd2ceaacf03cb41d5885f2a02c4d8804884918e3a258480803335",
        strip_prefix = "signature-1.3.1",
        build_file = Label("//cargo/remote:BUILD.signature-1.3.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__slab__0_4_4",
        url = "https://crates.io/api/v1/crates/slab/0.4.4/download",
        type = "tar.gz",
        sha256 = "c307a32c1c5c437f38c7fd45d753050587732ba8628319fbdf12a7e289ccc590",
        strip_prefix = "slab-0.4.4",
        build_file = Label("//cargo/remote:BUILD.slab-0.4.4.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__smallvec__1_6_1",
        url = "https://crates.io/api/v1/crates/smallvec/1.6.1/download",
        type = "tar.gz",
        sha256 = "fe0f37c9e8f3c5a4a66ad655a93c74daac4ad00c441533bf5c6e7990bb42604e",
        strip_prefix = "smallvec-1.6.1",
        build_file = Label("//cargo/remote:BUILD.smallvec-1.6.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__socket2__0_4_1",
        url = "https://crates.io/api/v1/crates/socket2/0.4.1/download",
        type = "tar.gz",
        sha256 = "765f090f0e423d2b55843402a07915add955e7d60657db13707a159727326cad",
        strip_prefix = "socket2-0.4.1",
        build_file = Label("//cargo/remote:BUILD.socket2-0.4.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__spin__0_5_2",
        url = "https://crates.io/api/v1/crates/spin/0.5.2/download",
        type = "tar.gz",
        sha256 = "6e63cff320ae2c57904679ba7cb63280a3dc4613885beafb148ee7bf9aa9042d",
        strip_prefix = "spin-0.5.2",
        build_file = Label("//cargo/remote:BUILD.spin-0.5.2.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__static_assertions__1_1_0",
        url = "https://crates.io/api/v1/crates/static_assertions/1.1.0/download",
        type = "tar.gz",
        sha256 = "a2eb9349b6444b326872e140eb1cf5e7c522154d69e7a0ffb0fb81c06b37543f",
        strip_prefix = "static_assertions-1.1.0",
        build_file = Label("//cargo/remote:BUILD.static_assertions-1.1.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__subtle__2_4_1",
        url = "https://crates.io/api/v1/crates/subtle/2.4.1/download",
        type = "tar.gz",
        sha256 = "6bdef32e8150c2a081110b42772ffe7d7c9032b606bc226c8260fd97e0976601",
        strip_prefix = "subtle-2.4.1",
        build_file = Label("//cargo/remote:BUILD.subtle-2.4.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__subtle_encoding__0_5_1",
        url = "https://crates.io/api/v1/crates/subtle-encoding/0.5.1/download",
        type = "tar.gz",
        sha256 = "7dcb1ed7b8330c5eed5441052651dd7a12c75e2ed88f2ec024ae1fa3a5e59945",
        strip_prefix = "subtle-encoding-0.5.1",
        build_file = Label("//cargo/remote:BUILD.subtle-encoding-0.5.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__syn__1_0_76",
        url = "https://crates.io/api/v1/crates/syn/1.0.76/download",
        type = "tar.gz",
        sha256 = "c6f107db402c2c2055242dbf4d2af0e69197202e9faacbef9571bbe47f5a1b84",
        strip_prefix = "syn-1.0.76",
        build_file = Label("//cargo/remote:BUILD.syn-1.0.76.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__synstructure__0_12_5",
        url = "https://crates.io/api/v1/crates/synstructure/0.12.5/download",
        type = "tar.gz",
        sha256 = "474aaa926faa1603c40b7885a9eaea29b444d1cb2850cb7c0e37bb1a4182f4fa",
        strip_prefix = "synstructure-0.12.5",
        build_file = Label("//cargo/remote:BUILD.synstructure-0.12.5.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__tempfile__3_2_0",
        url = "https://crates.io/api/v1/crates/tempfile/3.2.0/download",
        type = "tar.gz",
        sha256 = "dac1c663cfc93810f88aed9b8941d48cabf856a1b111c29a40439018d870eb22",
        strip_prefix = "tempfile-3.2.0",
        build_file = Label("//cargo/remote:BUILD.tempfile-3.2.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__termcolor__1_1_2",
        url = "https://crates.io/api/v1/crates/termcolor/1.1.2/download",
        type = "tar.gz",
        sha256 = "2dfed899f0eb03f32ee8c6a0aabdb8a7949659e3466561fc0adf54e26d88c5f4",
        strip_prefix = "termcolor-1.1.2",
        build_file = Label("//cargo/remote:BUILD.termcolor-1.1.2.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__testcontainers__0_12_0",
        url = "https://crates.io/api/v1/crates/testcontainers/0.12.0/download",
        type = "tar.gz",
        sha256 = "d5e3ed6e3598dbf32cba8cb356b881c085e0adea57597f387723430dd94b4084",
        strip_prefix = "testcontainers-0.12.0",
        build_file = Label("//cargo/remote:BUILD.testcontainers-0.12.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__thiserror__1_0_29",
        url = "https://crates.io/api/v1/crates/thiserror/1.0.29/download",
        type = "tar.gz",
        sha256 = "602eca064b2d83369e2b2f34b09c70b605402801927c65c11071ac911d299b88",
        strip_prefix = "thiserror-1.0.29",
        build_file = Label("//cargo/remote:BUILD.thiserror-1.0.29.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__thiserror_impl__1_0_29",
        url = "https://crates.io/api/v1/crates/thiserror-impl/1.0.29/download",
        type = "tar.gz",
        sha256 = "bad553cc2c78e8de258400763a647e80e6d1b31ee237275d756f6836d204494c",
        strip_prefix = "thiserror-impl-1.0.29",
        build_file = Label("//cargo/remote:BUILD.thiserror-impl-1.0.29.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__time__0_1_44",
        url = "https://crates.io/api/v1/crates/time/0.1.44/download",
        type = "tar.gz",
        sha256 = "6db9e6914ab8b1ae1c260a4ae7a49b6c5611b40328a735b21862567685e73255",
        strip_prefix = "time-0.1.44",
        build_file = Label("//cargo/remote:BUILD.time-0.1.44.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__tinyvec__1_3_1",
        url = "https://crates.io/api/v1/crates/tinyvec/1.3.1/download",
        type = "tar.gz",
        sha256 = "848a1e1181b9f6753b5e96a092749e29b11d19ede67dfbbd6c7dc7e0f49b5338",
        strip_prefix = "tinyvec-1.3.1",
        build_file = Label("//cargo/remote:BUILD.tinyvec-1.3.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__tinyvec_macros__0_1_0",
        url = "https://crates.io/api/v1/crates/tinyvec_macros/0.1.0/download",
        type = "tar.gz",
        sha256 = "cda74da7e1a664f795bb1f8a87ec406fb89a02522cf6e50620d016add6dbbf5c",
        strip_prefix = "tinyvec_macros-0.1.0",
        build_file = Label("//cargo/remote:BUILD.tinyvec_macros-0.1.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__tokio__1_11_0",
        url = "https://crates.io/api/v1/crates/tokio/1.11.0/download",
        type = "tar.gz",
        sha256 = "b4efe6fc2395938c8155973d7be49fe8d03a843726e285e100a8a383cc0154ce",
        strip_prefix = "tokio-1.11.0",
        build_file = Label("//cargo/remote:BUILD.tokio-1.11.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__tokio_macros__1_3_0",
        url = "https://crates.io/api/v1/crates/tokio-macros/1.3.0/download",
        type = "tar.gz",
        sha256 = "54473be61f4ebe4efd09cec9bd5d16fa51d70ea0192213d754d2d500457db110",
        strip_prefix = "tokio-macros-1.3.0",
        build_file = Label("//cargo/remote:BUILD.tokio-macros-1.3.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__tokio_native_tls__0_3_0",
        url = "https://crates.io/api/v1/crates/tokio-native-tls/0.3.0/download",
        type = "tar.gz",
        sha256 = "f7d995660bd2b7f8c1568414c1126076c13fbb725c40112dc0120b78eb9b717b",
        strip_prefix = "tokio-native-tls-0.3.0",
        build_file = Label("//cargo/remote:BUILD.tokio-native-tls-0.3.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__tokio_rustls__0_22_0",
        url = "https://crates.io/api/v1/crates/tokio-rustls/0.22.0/download",
        type = "tar.gz",
        sha256 = "bc6844de72e57df1980054b38be3a9f4702aba4858be64dd700181a8a6d0e1b6",
        strip_prefix = "tokio-rustls-0.22.0",
        build_file = Label("//cargo/remote:BUILD.tokio-rustls-0.22.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__tokio_tungstenite__0_15_0",
        url = "https://crates.io/api/v1/crates/tokio-tungstenite/0.15.0/download",
        type = "tar.gz",
        sha256 = "511de3f85caf1c98983545490c3d09685fa8eb634e57eec22bb4db271f46cbd8",
        strip_prefix = "tokio-tungstenite-0.15.0",
        build_file = Label("//cargo/remote:BUILD.tokio-tungstenite-0.15.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__tokio_util__0_6_8",
        url = "https://crates.io/api/v1/crates/tokio-util/0.6.8/download",
        type = "tar.gz",
        sha256 = "08d3725d3efa29485e87311c5b699de63cde14b00ed4d256b8318aa30ca452cd",
        strip_prefix = "tokio-util-0.6.8",
        build_file = Label("//cargo/remote:BUILD.tokio-util-0.6.8.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__toml__0_5_8",
        url = "https://crates.io/api/v1/crates/toml/0.5.8/download",
        type = "tar.gz",
        sha256 = "a31142970826733df8241ef35dc040ef98c679ab14d7c3e54d827099b3acecaa",
        strip_prefix = "toml-0.5.8",
        build_file = Label("//cargo/remote:BUILD.toml-0.5.8.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__tower_service__0_3_1",
        url = "https://crates.io/api/v1/crates/tower-service/0.3.1/download",
        type = "tar.gz",
        sha256 = "360dfd1d6d30e05fda32ace2c8c70e9c0a9da713275777f5a4dbb8a1893930c6",
        strip_prefix = "tower-service-0.3.1",
        build_file = Label("//cargo/remote:BUILD.tower-service-0.3.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__tracing__0_1_26",
        url = "https://crates.io/api/v1/crates/tracing/0.1.26/download",
        type = "tar.gz",
        sha256 = "09adeb8c97449311ccd28a427f96fb563e7fd31aabf994189879d9da2394b89d",
        strip_prefix = "tracing-0.1.26",
        build_file = Label("//cargo/remote:BUILD.tracing-0.1.26.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__tracing_core__0_1_19",
        url = "https://crates.io/api/v1/crates/tracing-core/0.1.19/download",
        type = "tar.gz",
        sha256 = "2ca517f43f0fb96e0c3072ed5c275fe5eece87e8cb52f4a77b69226d3b1c9df8",
        strip_prefix = "tracing-core-0.1.19",
        build_file = Label("//cargo/remote:BUILD.tracing-core-0.1.19.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__try_lock__0_2_3",
        url = "https://crates.io/api/v1/crates/try-lock/0.2.3/download",
        type = "tar.gz",
        sha256 = "59547bce71d9c38b83d9c0e92b6066c4253371f15005def0c30d9657f50c7642",
        strip_prefix = "try-lock-0.2.3",
        build_file = Label("//cargo/remote:BUILD.try-lock-0.2.3.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__tungstenite__0_14_0",
        url = "https://crates.io/api/v1/crates/tungstenite/0.14.0/download",
        type = "tar.gz",
        sha256 = "a0b2d8558abd2e276b0a8df5c05a2ec762609344191e5fd23e292c910e9165b5",
        strip_prefix = "tungstenite-0.14.0",
        build_file = Label("//cargo/remote:BUILD.tungstenite-0.14.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__typenum__1_14_0",
        url = "https://crates.io/api/v1/crates/typenum/1.14.0/download",
        type = "tar.gz",
        sha256 = "b63708a265f51345575b27fe43f9500ad611579e764c79edbc2037b1121959ec",
        strip_prefix = "typenum-1.14.0",
        build_file = Label("//cargo/remote:BUILD.typenum-1.14.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__unicode_bidi__0_3_6",
        url = "https://crates.io/api/v1/crates/unicode-bidi/0.3.6/download",
        type = "tar.gz",
        sha256 = "246f4c42e67e7a4e3c6106ff716a5d067d4132a642840b242e357e468a2a0085",
        strip_prefix = "unicode-bidi-0.3.6",
        build_file = Label("//cargo/remote:BUILD.unicode-bidi-0.3.6.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__unicode_normalization__0_1_19",
        url = "https://crates.io/api/v1/crates/unicode-normalization/0.1.19/download",
        type = "tar.gz",
        sha256 = "d54590932941a9e9266f0832deed84ebe1bf2e4c9e4a3554d393d18f5e854bf9",
        strip_prefix = "unicode-normalization-0.1.19",
        build_file = Label("//cargo/remote:BUILD.unicode-normalization-0.1.19.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__unicode_xid__0_2_2",
        url = "https://crates.io/api/v1/crates/unicode-xid/0.2.2/download",
        type = "tar.gz",
        sha256 = "8ccb82d61f80a663efe1f787a51b16b5a51e3314d6ac365b08639f52387b33f3",
        strip_prefix = "unicode-xid-0.2.2",
        build_file = Label("//cargo/remote:BUILD.unicode-xid-0.2.2.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__untrusted__0_7_1",
        url = "https://crates.io/api/v1/crates/untrusted/0.7.1/download",
        type = "tar.gz",
        sha256 = "a156c684c91ea7d62626509bce3cb4e1d9ed5c4d978f7b4352658f96a4c26b4a",
        strip_prefix = "untrusted-0.7.1",
        build_file = Label("//cargo/remote:BUILD.untrusted-0.7.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__url__2_2_2",
        url = "https://crates.io/api/v1/crates/url/2.2.2/download",
        type = "tar.gz",
        sha256 = "a507c383b2d33b5fc35d1861e77e6b383d158b2da5e14fe51b83dfedf6fd578c",
        strip_prefix = "url-2.2.2",
        build_file = Label("//cargo/remote:BUILD.url-2.2.2.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__utf_8__0_7_6",
        url = "https://crates.io/api/v1/crates/utf-8/0.7.6/download",
        type = "tar.gz",
        sha256 = "09cc8ee72d2a9becf2f2febe0205bbed8fc6615b7cb429ad062dc7b7ddd036a9",
        strip_prefix = "utf-8-0.7.6",
        build_file = Label("//cargo/remote:BUILD.utf-8-0.7.6.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__vcpkg__0_2_15",
        url = "https://crates.io/api/v1/crates/vcpkg/0.2.15/download",
        type = "tar.gz",
        sha256 = "accd4ea62f7bb7a82fe23066fb0957d48ef677f6eeb8215f372f52e48bb32426",
        strip_prefix = "vcpkg-0.2.15",
        build_file = Label("//cargo/remote:BUILD.vcpkg-0.2.15.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__version_check__0_9_3",
        url = "https://crates.io/api/v1/crates/version_check/0.9.3/download",
        type = "tar.gz",
        sha256 = "5fecdca9a5291cc2b8dcf7dc02453fee791a280f3743cb0905f8822ae463b3fe",
        strip_prefix = "version_check-0.9.3",
        build_file = Label("//cargo/remote:BUILD.version_check-0.9.3.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__waker_fn__1_1_0",
        url = "https://crates.io/api/v1/crates/waker-fn/1.1.0/download",
        type = "tar.gz",
        sha256 = "9d5b2c62b4012a3e1eca5a7e077d13b3bf498c4073e33ccd58626607748ceeca",
        strip_prefix = "waker-fn-1.1.0",
        build_file = Label("//cargo/remote:BUILD.waker-fn-1.1.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__want__0_3_0",
        url = "https://crates.io/api/v1/crates/want/0.3.0/download",
        type = "tar.gz",
        sha256 = "1ce8a968cb1cd110d136ff8b819a556d6fb6d919363c61534f6860c7eb172ba0",
        strip_prefix = "want-0.3.0",
        build_file = Label("//cargo/remote:BUILD.want-0.3.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__wasi__0_10_0_wasi_snapshot_preview1",
        url = "https://crates.io/api/v1/crates/wasi/0.10.0+wasi-snapshot-preview1/download",
        type = "tar.gz",
        sha256 = "1a143597ca7c7793eff794def352d41792a93c481eb1042423ff7ff72ba2c31f",
        strip_prefix = "wasi-0.10.0+wasi-snapshot-preview1",
        build_file = Label("//cargo/remote:BUILD.wasi-0.10.0+wasi-snapshot-preview1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__wasi__0_9_0_wasi_snapshot_preview1",
        url = "https://crates.io/api/v1/crates/wasi/0.9.0+wasi-snapshot-preview1/download",
        type = "tar.gz",
        sha256 = "cccddf32554fecc6acb585f82a32a72e28b48f8c4c1883ddfeeeaa96f7d8e519",
        strip_prefix = "wasi-0.9.0+wasi-snapshot-preview1",
        build_file = Label("//cargo/remote:BUILD.wasi-0.9.0+wasi-snapshot-preview1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__wasm_bindgen__0_2_76",
        url = "https://crates.io/api/v1/crates/wasm-bindgen/0.2.76/download",
        type = "tar.gz",
        sha256 = "8ce9b1b516211d33767048e5d47fa2a381ed8b76fc48d2ce4aa39877f9f183e0",
        strip_prefix = "wasm-bindgen-0.2.76",
        build_file = Label("//cargo/remote:BUILD.wasm-bindgen-0.2.76.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__wasm_bindgen_backend__0_2_76",
        url = "https://crates.io/api/v1/crates/wasm-bindgen-backend/0.2.76/download",
        type = "tar.gz",
        sha256 = "cfe8dc78e2326ba5f845f4b5bf548401604fa20b1dd1d365fb73b6c1d6364041",
        strip_prefix = "wasm-bindgen-backend-0.2.76",
        build_file = Label("//cargo/remote:BUILD.wasm-bindgen-backend-0.2.76.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__wasm_bindgen_macro__0_2_76",
        url = "https://crates.io/api/v1/crates/wasm-bindgen-macro/0.2.76/download",
        type = "tar.gz",
        sha256 = "44468aa53335841d9d6b6c023eaab07c0cd4bddbcfdee3e2bb1e8d2cb8069fef",
        strip_prefix = "wasm-bindgen-macro-0.2.76",
        build_file = Label("//cargo/remote:BUILD.wasm-bindgen-macro-0.2.76.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__wasm_bindgen_macro_support__0_2_76",
        url = "https://crates.io/api/v1/crates/wasm-bindgen-macro-support/0.2.76/download",
        type = "tar.gz",
        sha256 = "0195807922713af1e67dc66132c7328206ed9766af3858164fb583eedc25fbad",
        strip_prefix = "wasm-bindgen-macro-support-0.2.76",
        build_file = Label("//cargo/remote:BUILD.wasm-bindgen-macro-support-0.2.76.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__wasm_bindgen_shared__0_2_76",
        url = "https://crates.io/api/v1/crates/wasm-bindgen-shared/0.2.76/download",
        type = "tar.gz",
        sha256 = "acdb075a845574a1fa5f09fd77e43f7747599301ea3417a9fbffdeedfc1f4a29",
        strip_prefix = "wasm-bindgen-shared-0.2.76",
        build_file = Label("//cargo/remote:BUILD.wasm-bindgen-shared-0.2.76.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__web_sys__0_3_53",
        url = "https://crates.io/api/v1/crates/web-sys/0.3.53/download",
        type = "tar.gz",
        sha256 = "224b2f6b67919060055ef1a67807367c2066ed520c3862cc013d26cf893a783c",
        strip_prefix = "web-sys-0.3.53",
        build_file = Label("//cargo/remote:BUILD.web-sys-0.3.53.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__webpki__0_21_4",
        url = "https://crates.io/api/v1/crates/webpki/0.21.4/download",
        type = "tar.gz",
        sha256 = "b8e38c0608262c46d4a56202ebabdeb094cef7e560ca7a226c6bf055188aa4ea",
        strip_prefix = "webpki-0.21.4",
        build_file = Label("//cargo/remote:BUILD.webpki-0.21.4.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__webpki_roots__0_21_1",
        url = "https://crates.io/api/v1/crates/webpki-roots/0.21.1/download",
        type = "tar.gz",
        sha256 = "aabe153544e473b775453675851ecc86863d2a81d786d741f6b76778f2a48940",
        strip_prefix = "webpki-roots-0.21.1",
        build_file = Label("//cargo/remote:BUILD.webpki-roots-0.21.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__winapi__0_3_9",
        url = "https://crates.io/api/v1/crates/winapi/0.3.9/download",
        type = "tar.gz",
        sha256 = "5c839a674fcd7a98952e593242ea400abe93992746761e38641405d28b00f419",
        strip_prefix = "winapi-0.3.9",
        build_file = Label("//cargo/remote:BUILD.winapi-0.3.9.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__winapi_i686_pc_windows_gnu__0_4_0",
        url = "https://crates.io/api/v1/crates/winapi-i686-pc-windows-gnu/0.4.0/download",
        type = "tar.gz",
        sha256 = "ac3b87c63620426dd9b991e5ce0329eff545bccbbb34f3be09ff6fb6ab51b7b6",
        strip_prefix = "winapi-i686-pc-windows-gnu-0.4.0",
        build_file = Label("//cargo/remote:BUILD.winapi-i686-pc-windows-gnu-0.4.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__winapi_util__0_1_5",
        url = "https://crates.io/api/v1/crates/winapi-util/0.1.5/download",
        type = "tar.gz",
        sha256 = "70ec6ce85bb158151cae5e5c87f95a8e97d2c0c4b001223f33a334e3ce5de178",
        strip_prefix = "winapi-util-0.1.5",
        build_file = Label("//cargo/remote:BUILD.winapi-util-0.1.5.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__winapi_x86_64_pc_windows_gnu__0_4_0",
        url = "https://crates.io/api/v1/crates/winapi-x86_64-pc-windows-gnu/0.4.0/download",
        type = "tar.gz",
        sha256 = "712e227841d057c1ee1cd2fb22fa7e5a5461ae8e48fa2ca79ec42cfc1931183f",
        strip_prefix = "winapi-x86_64-pc-windows-gnu-0.4.0",
        build_file = Label("//cargo/remote:BUILD.winapi-x86_64-pc-windows-gnu-0.4.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__xxhash_rust__0_8_2",
        url = "https://crates.io/api/v1/crates/xxhash-rust/0.8.2/download",
        type = "tar.gz",
        sha256 = "e575e15bedf6e57b5c2d763ffc6c3c760143466cbd09d762d539680ab5992ded",
        strip_prefix = "xxhash-rust-0.8.2",
        build_file = Label("//cargo/remote:BUILD.xxhash-rust-0.8.2.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__yaml_rust__0_4_5",
        url = "https://crates.io/api/v1/crates/yaml-rust/0.4.5/download",
        type = "tar.gz",
        sha256 = "56c1936c4cc7a1c9ab21a1ebb602eb942ba868cbd44a99cb7cdc5892335e1c85",
        strip_prefix = "yaml-rust-0.4.5",
        build_file = Label("//cargo/remote:BUILD.yaml-rust-0.4.5.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__zeroize__1_4_1",
        url = "https://crates.io/api/v1/crates/zeroize/1.4.1/download",
        type = "tar.gz",
        sha256 = "377db0846015f7ae377174787dd452e1c5f5a9050bc6f954911d01f116daa0cd",
        strip_prefix = "zeroize-1.4.1",
        build_file = Label("//cargo/remote:BUILD.zeroize-1.4.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__zeroize_derive__1_1_0",
        url = "https://crates.io/api/v1/crates/zeroize_derive/1.1.0/download",
        type = "tar.gz",
        sha256 = "a2c1e130bebaeab2f23886bf9acbaca14b092408c452543c857f66399cd6dab1",
        strip_prefix = "zeroize_derive-1.1.0",
        build_file = Label("//cargo/remote:BUILD.zeroize_derive-1.1.0.bazel"),
    )
