use strict;
use warnings;
use Module::Build;

my $build = Module::Build->new(
    license        => 'perl',
    dynamic_config => 0,

    build_requires     => {
        'Test::More'  => '0.98',
        'Tie::IxHash' => '0',
    },
    configure_requires => { 'Module::Build' => '0.38' },
    requires           => { 'perl' => '5.008001' },

    no_index    => { 'directory' => [ 'inc', 't', 'xt' ] },
    name        => 'SQL-Format',
    module_name => 'SQL::Format',

    test_files           => ( -d '.git' || $ENV{RELEASE_TESTING} ) ? 't/ xt/' : 't/',
    recursive_test_files => 1,

    meta_merge => {
        resources => {
            repository => 'http://github.com/xaicron/p5-SQL-Format',
            bugtracker => 'http://github.com/xaicron/p5-SQL-Format/issues',
        },
    },

    create_readme  => 1,
    create_license => 1,
);

$build->create_build_script();
