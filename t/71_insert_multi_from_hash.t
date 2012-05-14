use strict;
use warnings;
use t::Util;
use Test::More;

my $test = mk_test 'insert_multi_from_hash';

$test->(
    desc => 'basic',
    input   => [
        foo => [
            { bar => 'hoge', baz => 'fuga' },
            { bar => 'fizz', baz => 'buzz' },
        ],
    ],
    expects => {
        stmt => 'INSERT INTO `foo` (`bar`, `baz`) VALUES (?, ?), (?, ?)',
        bind => [qw/hoge fuga fizz buzz/],
    },
);

$test->(
    desc => 'mismatch params',
    input   => [
        foo => [
            { bar => 'hoge', baz => 'fuga' },
            { bar => 'fizz', baz => 'buzz', xxx => 'yyy' },
            { },
        ],
    ],
    expects => {
        stmt => 'INSERT INTO `foo` (`bar`, `baz`) VALUES (?, ?), (?, ?), (?, ?)',
        bind => [qw/hoge fuga fizz buzz/, undef, undef],
    },
);

$test->(
    desc => 'complex',
    input   => [
        foo => [
            { bar => 'hoge', baz => \'NOW()' },
            { bar => 'fuga', baz => \['UNIX_TIMESTAMP(?)', '2012-12-12'] },
        ],
    ],
    expects => {
        stmt => 'INSERT INTO `foo` (`bar`, `baz`) VALUES (?, NOW()), (?, UNIX_TIMESTAMP(?))',
        bind => [qw/hoge fuga 2012-12-12/],
    },
);

$test->(
    desc => 'insert ignore',
    input   => [
        foo => [
            { bar => 'hoge', baz => 'fuga' },
            { bar => 'fizz', baz => 'buzz' },
        ],
        { prefix => 'INSERT IGNORE INTO' },
    ],
    expects => {
        stmt => 'INSERT IGNORE INTO `foo` (`bar`, `baz`) VALUES (?, ?), (?, ?)',
        bind => [qw/hoge fuga fizz buzz/],
    },
);

$test->(
    desc => 'on duplicate key update',
    input   => [
        foo => [
            { bar => 'hoge', baz => 'fuga' },
            { bar => 'fizz', baz => 'buzz' },
        ],
        { update => { bar => 'piyo' } },
    ],
    expects => {
        stmt => 'INSERT INTO `foo` (`bar`, `baz`) VALUES (?, ?), (?, ?) ON DUPLICATE KEY UPDATE `bar` = ?',
        bind => [qw/hoge fuga fizz buzz piyo/],
    },
);

done_testing;
