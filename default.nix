{
  pkgs ? import <nixpkgs> { },
  lib ? pkgs.lib,
  docutils ? pkgs.docutils,
  jq ? pkgs.jq,
  lzo ? pkgs.lzo,
  pkgconfig ? pkgs.pkg-config,
  rustPlatform ? pkgs.rustPlatform,
  stdenv ? pkgs.stdenv,
}:

let
  excludeTarget =
    name: type:
    let
      baseName = baseNameOf (toString name);
    in
    !(baseName == "target" && type == "directory");

in
rustPlatform.buildRustPackage rec {
  name = "backy-extract";
  version = "1.1.0";

  src = lib.cleanSourceWith {
    filter = n: t: (excludeTarget n t) && (lib.cleanSourceFilter n t);
    src = ./.;
  };

  nativeBuildInputs = [
    docutils
    jq
    pkgconfig
  ];

  buildInputs = [
    lzo
  ];

  preConfigure = ''
    if ! cargo read-manifest | jq .version -r | grep -q $version; then
      echo "*** version mismatch, expected $version in Cargo.toml" >&2
      false
    fi
  '';

  cargoHash = "sha256-/SkFm/Kq8dRszloJC3ZAEYfpF32uc5CWfpGl1u3tBFA=";
  cargoBuildFlags = [ "--features fuse_driver" ];
  checkType = "debug";

  postPatch = ''
    substituteAllInPlace man/*.rst
  '';

  postBuild = ''
    mkdir -p $out/share/doc $out/share/man/man1
    cp README.md $out/share/doc
    for f in man/*.1.rst; do
      base="''${f#*/}"
      rst2man $f > "$out/share/man/man1/''${base%%.rst}"
    done
  '';

  meta = with lib; {
    description = "Rapid restore tool for backy";
    license = licenses.bsd3;
    platforms = platforms.unix;
  };
}
