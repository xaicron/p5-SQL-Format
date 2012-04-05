use strict;
use warnings;
use Test::More;

use SQL::Format;

subtest 'no args' => sub {
    my $f = SQL::Format->new;
    is $f->{separator}, $SQL::Format::SEPARATOR;
    is $f->{name_sep}, $SQL::Format::NAME_SEP;
    is $f->{quote_char}, $SQL::Format::QUOTE_CHAR;
};

subtest 'set all' => sub {
    my $f = SQL::Format->new(
        separator  => ',',
        name_sep   => '',
        quote_char =>  '',
    );
    is $f->{separator}, ',';
    is $f->{name_sep}, '';
    is $f->{quote_char}, '';
};

done_testing;
