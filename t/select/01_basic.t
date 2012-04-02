use strict;
use warnings;
use Test::More;

use SQL::Format;

sub test_select {
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    my %specs = @_;
    my ($input, $expects, $desc, $instance) =
        @specs{qw/input expects desc instance/};

    $instance ||= SQL::Format->new;
    subtest $desc => sub {
        my ($stmt, @bind) = $instance->select(@$input);
        is $stmt, $expects->{stmt};
        is_deeply \@bind, $expects->{bind};
    };
}

test_select(
    desc    => 'no conditions',
    input   => [foo => 'bar'],
    expects => {
        stmt => 'SELECT `bar` FROM `foo`',
        bind => [],
    },
);

test_select(
    desc    => 'no conditions (astarisk)',
    input   => [foo => '*'],
    expects => {
        stmt => 'SELECT * FROM `foo`',
        bind => [],
    },
);

test_select(
    desc    => 'no conditions (multi columns)',
    input   => [foo => [qw/bar baz/]],
    expects => {
        stmt => 'SELECT `bar`, `baz` FROM `foo`',
        bind => [],
    },
);

test_select(
    desc    => 'add where',
    input   => [foo => [qw/bar baz/], { hoge => 'fuga' }],
    expects => {
        stmt => 'SELECT `bar`, `baz` FROM `foo` WHERE (`hoge` = ?)',
        bind => [qw/fuga/],
    },
);

test_select(
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

test_select(
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

done_testing;
