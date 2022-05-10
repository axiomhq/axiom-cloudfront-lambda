let pkgs = import <nixpkgs> {};
in pkgs.mkShell {
  nativeBuildInputs = [ pkgs.go_1_18 ];
}
