use strict;
use warnings;
use t::Util;
use Test::More;
use SQL::Format;

subtest '%c: null' => sub {
    eval { sqlf 'SELECT %c FROM table' };
    like $@, mk_errstr '%c' => 'columns';
};

subtest '%t: undef' => sub {
    eval { sqlf 'SELECT foo FROM %t' };
    like $@, mk_errstr '%t', 'table';
};

subtest '%t: null' => sub {
    eval { sqlf 'SELECT foo FROM %t' };
    like $@, mk_errstr '%t', 'table';
};

subtest '%w: no-op equals OR' => sub {
    my ($stmt, @bind) = sqlf 'WHERE %w', {
        where => {
            id => [{ '>' => 10 }, { '<' => '20' } ],
        },
    };
    is $stmt, 'WHERE (`id` > ?) OR (`id` < ?)';
    is_deeply \@bind, [qw/10 20/];
};

done_testing;
