{ system ? builtins.system
, pkgs ? import <nixpkgs> {}
, stdenv ? pkgs.stdenv
, lib ? pkgs.lib
, docutils ? pkgs.docutils
, fuse ? pkgs.fuse
, jq ? pkgs.jq
, lzo ? pkgs.lzo
, pkgconfig ? pkgs.pkgconfig
, rustPlatform ? pkgs.rustPlatform
, Security ? pkgs.darwin.apple_sdk.frameworks.Security
}:

let
  excludeTarget =
    name: type: let baseName = baseNameOf (toString name); in ! (
      baseName == "target" && type == "directory");

in
rustPlatform.buildRustPackage rec {
  name = "backy-extract";
  version = "1.1.0";

  src = lib.cleanSourceWith {
    filter = n: t: (excludeTarget n t) && (lib.cleanSourceFilter n t);
    src = ./.;
  };

  nativeBuildInputs = [ docutils jq pkgconfig ];

  buildInputs =
    [ lzo ] ++
    (lib.optionals stdenv.isDarwin [ Security ]) ++
    (lib.optionals stdenv.isLinux [ fuse ]);

  preConfigure = ''
    if ! cargo read-manifest | jq .version -r | grep -q $version; then
      echo "*** version mismatch, expected $version in Cargo.toml" >&2
      false
    fi
  '';

  cargoHash = "sha256-fbm0jYQWb7WG/yMH4OCNMMc2EXuH2KG+7vUa/jDwUjU=";
  cargoBuildFlags = lib.optionals stdenv.isLinux [ "--features fuse_driver" ];
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
    maintainers = [ maintainers.ckauhaus ];
    platforms = platforms.unix;
  };
}
