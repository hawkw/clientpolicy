scope@{ pkgs ? import <nixpkgs> { } }:

let env = (import ./default.nix scope);

in pkgs.mkShell {
  PROTOC = "${pkgs.protobuf}/bin/protoc";
  PROTOC_INCLUDE = "${pkgs.protobuf}/include";
  LOCALE_ARCHIVE = "${pkgs.glibcLocales}/lib/locale/locale-archive";
  LC_ALL = "en_US.UTF-8";
  OPENSSL_DIR = "${pkgs.openssl.dev}";
  OPENSSL_LIB_DIR = "${pkgs.openssl.out}/lib";
  # SSL_CERT_FILE = "${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt";
  # GIT_SSL_CAINFO = "${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt";
  # CURL_CA_BUNDLE = "${pkgs.cacert}/etc/ca-bundle.crt";
  CARGO_TERM_COLOR = "always";
  AWS_ACCESS_KEY_ID = "dummy";
  AWS_SECRET_ACCESS_KEY = "dummy";
  RUST_BACKTRACE = "full";
  LINKERD_TEST_PRETTY_DIFF = "1";
  buildInputs = with pkgs; [
    (import ./default.nix { inherit pkgs; })
    yq-go
    kube3d
    just
    docker
  ];
}
