use strict;
use warnings;
use t::Util;
use Test::More;
use SQL::Format;

subtest '%t: undef' => sub {
    eval { sqlf 'SELECT foo FROM %t' };
    like $@, mk_errstr 1, '%t';
};

subtest '%w: undef' => sub {
    eval {
        sqlf 'SELECT %c FROM %t WHERE %w', (
            [qw/bar baz/], 'foo',
        );
    };
    like $@, mk_errstr 3, '%w';
};

done_testing;
