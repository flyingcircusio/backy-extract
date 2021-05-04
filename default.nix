{ system ? builtins.system
, pkgs ? import <nixpkgs> {}
, stdenv ? pkgs.stdenv
, lib ? pkgs.lib
, lzo ? pkgs.lzo
, fuse ? pkgs.fuse
, pkgconfig ? pkgs.pkgconfig
, docutils ? pkgs.docutils
, jq ? pkgs.jq
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
  version = "0.3.2";

  src = lib.cleanSourceWith {
    filter = n: t: (excludeTarget n t) && (lib.cleanSourceFilter n t);
    src = ./.;
  };

  nativeBuildInputs = [ docutils jq pkgconfig ];

  buildInputs =
    [ lzo ] ++
    (lib.optionals stdenv.isDarwin [ Security ]) ++
    (lib.optionals stdenv.isLinux [ pkgconfig fuse ]);

  preConfigure = ''
    if ! cargo read-manifest | jq .version -r | grep -q $version; then
      echo "*** version mismatch, expected $version in Cargo.toml" >&2
      false
    fi
  '';

  cargoSha256 = "1bk6cy0qm0j2wr7qld08gjgmmsvnbz15ifk3ivms9xkhzlfhrz3v";
  cargoBuildFlags = lib.optionals stdenv.isLinux [ "--features fuse_driver" ];

  meta = with lib; {
    description = "Rapid restore tool for backy";
    license = licenses.bsd3;
    maintainers = [ maintainers.ckauhaus ];
    platforms = platforms.unix;
  };

  postPatch = ''
    substituteAllInPlace man/*.rst
  '';

  postBuild = ''
    mkdir -p $out/share/doc
    cp README.md $out/share/doc

    mkdir -p $out/share/man/man1
    for f in $man/*.1.rst; do
      rst2man $f > $out/share/man/man1/''${f%%.rst}
    done
  '';
}
