{ system ? builtins.system
, lib ? pkgs.lib
, lzo ? pkgs.lzo
, pkgs ? import <nixpkgs> {}
, rustPlatform ? pkgs.rustPlatform
, Security ? pkgs.darwin.apple_sdk.frameworks.Security
, stdenv ? pkgs.stdenv
}:

rustPlatform.buildRustPackage rec {
  name = "backy-extract";
  version = "0.3.0";

  src = lib.cleanSource ./.;
  buildInputs = [ lzo ] ++ lib.optionals stdenv.isDarwin [ Security ];
  cargoSha256 = "10lzz328mzq33gpsxmxy6q9xkyhxpfn6dnj5zc9x4kmaimzpbipw";

  meta = with lib; {
    description = "Rapid restore tool for backy";
    license = licenses.bsd3;
    maintainers = [ maintainers.ckauhaus ];
    platforms = platforms.unix;
  };
}
