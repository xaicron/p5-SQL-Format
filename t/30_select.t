use strict;
use warnings;
use t::Util;
use Test::More;

my $test = mk_test 'select';

$test->(
    desc    => 'no conditions',
    input   => [foo => 'bar'],
    expects => {
        stmt => 'SELECT `bar` FROM `foo`',
        bind => [],
    },
);

$test->(
    desc    => 'no conditions (astarisk)',
    input   => [foo => '*'],
    expects => {
        stmt => 'SELECT * FROM `foo`',
        bind => [],
    },
);

$test->(
    desc    => 'no conditions (multi columns)',
    input   => [foo => [qw/bar baz/]],
    expects => {
        stmt => 'SELECT `bar`, `baz` FROM `foo`',
        bind => [],
    },
);

$test->(
    desc    => 'add where',
    input   => [foo => [qw/bar baz/], { hoge => 'fuga' }],
    expects => {
        stmt => 'SELECT `bar`, `baz` FROM `foo` WHERE (`hoge` = ?)',
        bind => [qw/fuga/],
    },
);

$test->(
    desc    => 'add where, add order by',
    input   => [
        foo => [qw/bar baz/],
        { hoge => 'fuga' },
        { order_by => 'piyo' },
    ],
    expects => {
        stmt => 'SELECT `bar`, `baz` FROM `foo` WHERE (`hoge` = ?) ORDER BY `piyo`',
        bind => [qw/fuga/],
    },
);

$test->(
    desc => 'limmit offset',
    input => [
        foo => '*',
        undef,
        { limit => 1, offset => 2 },
    ],
    expects => {
        stmt => 'SELECT * FROM `foo` LIMIT 1 OFFSET 2',
        bind => [],
    },
);

$test->(
    desc  => 'custom prefix',
    input => [
        foo => '*',
        undef,
        { prefix => 'SELECT SQL_CALC_FOUND_ROWS' },
    ],
    expects => {
        stmt => 'SELECT SQL_CALC_FOUND_ROWS * FROM `foo`',
        bind => [],
    },
);

done_testing;
