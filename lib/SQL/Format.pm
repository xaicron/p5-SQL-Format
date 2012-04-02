package SQL::Format;

use strict;
use warnings;
use 5.008_001;
our $VERSION = '0.01';

use Exporter 'import';
use Carp qw(croak carp);

our @EXPORT = qw(sqlf);

our $SEPARATOR     = ', ';
our $NAME_SEP      = '.';
our $QUOTE_CHAR    = '`';
our $LIMIT_DIALECT = 'LimitOffset';

my $FMAP = {
    '%c' => 'columns',
    '%t' => 'table',
    '%w' => 'where',
    '%o' => 'options', # order_by, limit, offset
};

my $OP_ALIAS = {
    -IN          => 'IN',
    -NOT_IN      => 'NOT IN',
    -BETWEEN     => 'BETWEEN',
    -NOT_BETWEEN => 'NOT BETWEEN',
    -LIKE        => 'LIKE',
    -NOT_LIKE    => 'NOT LIKE',
};

use constant {
    _LIMIT_OFFSET => 1,
    _LIMIT_XY     => 2,
    _LIMIT_YX     => 3,
};
my $LIMIT_DIALECT_MAP = {
    LimitOffset => _LIMIT_OFFSET, # PostgreSQL, SQLite, MySQL 5.0
    LimitXY     => _LIMIT_XY,     # MySQL
    LimitYX     => _LIMIT_YX,     # SQLite
};

sub sqlf {
    my ($format, $args) = @_;
    $args ||= {};

    my @bind;
    for my $key (keys $args) {
        my $val = $args->{$key};
        if ($key eq 'columns') {
            if (!defined $val) {
                $args->{$key} = '*';
            }
            elsif (ref $val eq 'ARRAY') {
                if (@$val) {
                    $args->{$key} = join $SEPARATOR, map { _quote($_) } @$val;
                }
                else {
                    $args->{$key} = '*';
                }
            }
            elsif (ref $val eq 'SCALAR') {
                $args->{$key} = $$val;
            }
            else {
                $args->{$key} = _quote($val);
            }
        }
        elsif ($key eq 'table') {
            if (ref $val eq 'ARRAY') {
                $args->{$key} = join $SEPARATOR, map { _quote($_) } @$val;
            }
            elsif (defined $val) {
                $args->{$key} = _quote($val);
            }
        }
        # taken from SQL::Maker
        elsif ($key eq 'where' && ref $val eq 'HASH') {
            $args->{$key} = join ' AND ', map {
                my $org_key  = $_;
                my $no_paren = 0;
                my ($k, $v) = (_quote($org_key), $val->{$_});
                if (ref $v eq 'ARRAY')  {
                    if (
                           ref $v->[0]
                        or (($v->[0]||'') eq '-and')
                        or (($v->[0]||'') eq '-or')
                    ) {
                        # [-and => qw/foo bar baz/]
                        # [-and => { '>' => 10 }, { '<' => 20 } ]
                        # [-or  => qw/foo bar baz/]
                        # [-or  => { '>' => 10 }, { '<' => 20 } ]
                        # [{ '>' => 10 }, { '<' => 20 } ]
                        my $logic = 'OR';
                        my @values = @$v;
                        if ($v->[0] && $v->[0] eq '-and') {
                            $logic = 'AND';
                            shift @values;
                        }
                        elsif ($v->[0] && $v->[0] eq '-or') {
                            shift @values;
                        }
                        my @statements;
                        for my $arg (@values) {
                            my ($_stmt, @_bind) = sqlf('%w', {
                                where => { $org_key => $arg },
                            });
                            push @statements, $_stmt;
                            push @bind, @_bind;
                        }
                        $k = join " $logic ", @statements;
                        $no_paren = 1;
                    }
                    elsif (@$v == 0) {
                        # []
                        $k = '0=1';
                    }
                    else {
                        # [qw/1 2 3/]
                        $k .= ' IN ('.join($SEPARATOR, ('?')x@$v).')';
                        push @bind, @$v;
                    }
                }
                elsif (ref $v eq 'HASH') {
                    (my $op, $v) = %$v;
                    $op = uc $op;
                    $op = $OP_ALIAS->{$op} || $op;
                    if ($op eq 'IN' || $op eq 'NOT IN') {
                        my $ref = ref $v;
                        if ($ref eq 'ARRAY') {
                            unless (@$v) {
                                # { IN => [] }
                                $k = $op eq 'IN' ? '0=1' : '1=1';
                            }
                            else {
                                # { IN => [qw/1 2 3/] }
                                $k .= " $op (".join($SEPARATOR, ('?')x@$v).')';
                                push @bind, @$v;
                            }
                        }
                        elsif ($ref eq 'REF') {
                            # { IN => \['SELECT foo FROM bar WHERE hoge = ?', 'fuga']
                            $k .= " $op (${$v}->[0])";
                            push @bind, @{$$v}[1..$#$$v];
                        }
                        elsif ($ref eq 'SCALAR') {
                            # { IN => \'SELECT foo FROM bar' }
                            $k .= " $op ($$v)";
                        }
                        elsif (defined $v) {
                            # { IN => 'foo' }
                            $k .= $op eq 'IN' ? ' = ?' : ' <> ?';
                            push @bind, $v;
                        }
                        else {
                            # { IN => undef }
                            $k .= $op eq 'IN' ? ' IS NULL' : ' IS NOT NULL';
                        }
                    }
                    elsif ($op eq 'BETWEEN' || $op eq 'NOT BETWEEN') {
                        my $ref = ref $v;
                        if ($ref eq 'ARRAY') {
                            # { BETWEEN => ['foo', 'bar'] }
                            # { BETWEEN => [\'lower(x)', \['upper(?)', 'y']] }
                            my ($va, $vb) = @$v;
                            my @stmt;
                            for my $value ($va, $vb) {
                                if (ref $value eq 'SCALAR') {
                                    push @stmt, $$value;
                                }
                                elsif (ref $value eq 'REF') {
                                    push @stmt, ${$value}->[0];
                                    push @bind, @{$$value}[1..$#$$value];
                                }
                                else {
                                    push @stmt, '?';
                                    push @bind, $value;
                                }
                            }
                            $k .= " $op ".join ' AND ', @stmt;
                        }
                        elsif ($ref eq 'REF') {
                            # { BETWEEN => \["? AND ?", 1, 2] }
                            $k .= " $op ${$v}->[0]";
                            push @bind, @{$$v}[1..$#$$v];
                        }
                        elsif ($ref eq 'SCALAR') {
                            # { BETWEEN => \'lower(x) AND upper(y)' }
                            $k .= " $op $$v";
                        }
                        else {
                            # { BETWEEN => $scalar }
                            # noop
                        }
                    }
                    elsif ($op eq 'LIKE' || $op eq 'NOT LIKE') {
                        my $ref = ref $v;
                        if ($ref eq 'ARRAY') {
                            # { LIKE => ['%foo', 'bar%'] }
                            # { LIKE => [\'%foo', \'bar%'] }
                            my @stmt;
                            for my $value (@$v) {
                                if (ref $value eq 'SCALAR') {
                                    push @stmt, $$value;
                                }
                                else {
                                    push @stmt, '?';
                                    push @bind, $value;
                                }
                            }
                            $k = join ' OR ', map { "$k $op $_" } @stmt;
                        }
                        elsif ($ref eq 'SCALAR') {
                            # { LIKE => \'"foo%"' }
                            $k .= " $op $$v";
                        }
                        else {
                            $k .= " $op ?";
                            push @bind, $v;
                        }
                    }
                    elsif (ref $v eq 'SCALAR') {
                        # { '>' => \'foo' }
                        $k .= " $op ($$v)";
                    }
                    else {
                        # { '>' => 'foo' }
                        $k .= " $op ?";
                        push @bind, $v;
                    }
                }
                elsif (ref $v eq 'SCALAR') {
                    # \'foo'
                    $k .= " $$v";
                }
                elsif (!defined $v) {
                    # undef
                    $k .= ' IS NULL';
                }
                else {
                    # 'foo'
                    $k .= ' = ?';
                    push @bind, $v;
                }
                $no_paren ? $k : "($k)";
            } sort keys %$val;
        }
        elsif ($key eq 'options' && ref $val eq 'HASH') {
            my @exprs;
            if (defined(my $group_by = $val->{group_by})) {
                # TODO
            }
            if (defined(my $having = $val->{having})) {
                # TODO
            }
            if (defined(my $order_by = $val->{order_by})) {
                my $ret = 'ORDER BY ';
                if (ref $order_by eq 'HASH') {
                    # order_by => { column => 'DESC' }
                    $ret .= join $SEPARATOR, map {
                        _quote($_).' '.$order_by->{$_}
                    } sort keys %$order_by;
                }
                elsif (ref $order_by eq 'ARRAY') {
                    # order_by => ['column1', { column2 => 'DESC' }]
                    my @parts;
                    for my $part (@$order_by) {
                        if (ref $part eq 'HASH') {
                            push @parts, join $SEPARATOR, map {
                                _quote($_).' '.$part->{$_}
                            } sort keys %$part;
                        }
                        else {
                            push @parts, _quote($part);
                        }
                    }
                    $ret .= join $SEPARATOR, @parts;
                }
                else {
                    # order_by => 'column'
                    $ret .= _quote($order_by);
                }
                push @exprs, $ret;
            }
            if (defined $val->{limit}) {
                my $ret = 'LIMIT ';
                if ($val->{offset}) { # defined and > 0
                    my $limit_dialect = $LIMIT_DIALECT_MAP->{$LIMIT_DIALECT} || 0;
                    if ($limit_dialect == _LIMIT_OFFSET) {
                        $ret .= "$val->{limit} OFFSET $val->{offset}";
                    }
                    elsif ($limit_dialect == _LIMIT_XY) {
                        $ret .= "$val->{offset}, $val->{limit}";
                    }
                    elsif ($limit_dialect == _LIMIT_YX) {
                        $ret .= "$val->{limit}, $val->{offset}";
                    }
                    else {
                        warn "Unkown LIMIT_DIALECT `$LIMIT_DIALECT`";
                        $ret .= $val->{limit};
                    }
                }
                else {
                    $ret .= $val->{limit};
                }
                push @exprs, $ret;
            }

            $args->{$key} = join ' ', @exprs;
        }
    }

    my @tokens = split m#(%[ctwo])(?=\W|$)#, $format;
    for (my $i = 1; $i < @tokens; $i += 2) {
        my $org = $tokens[$i];
        $tokens[$i] = $args->{$FMAP->{$org}};
        croak "'$org' must be specified '$FMAP->{$org}' field"
            unless defined $tokens[$i];
    }

    return join('',@tokens), @bind;
}

sub _quote {
    my $stuff = shift;
    return $$stuff if ref $stuff eq 'SCALAR';
    return $stuff unless $QUOTE_CHAR && $NAME_SEP;
    return $stuff if $stuff eq '*';
    return $stuff if substr($stuff, 0, 1) eq $QUOTE_CHAR; # skip maybe quoted
    return join $NAME_SEP, map {
        "$QUOTE_CHAR$_$QUOTE_CHAR"
    } split /\Q$NAME_SEP\E/, $stuff;
}

sub new {
    my ($class, %args) = @_;
    $args{separator}     = $SEPARATOR     unless defined $args{separator};
    $args{name_sep}      = $NAME_SEP      unless defined $args{name_sep};
    $args{quote_char}    = $QUOTE_CHAR    unless defined $args{quote_char};
    $args{limit_dialect} = $LIMIT_DIALECT unless defined $args{limit_dialect};
    bless { %args }, $class;
}

sub select {
    my ($self, $table, $cols, $where, $opts) = @_;
    croak 'Usage: $sqlf->select($table [, \@cols, \%where, \%opts])' unless defined $table;

    local $SEPARATOR     = $self->{separator};
    local $NAME_SEP      = $self->{name_sep};
    local $QUOTE_CHAR    = $self->{quote_char};
    local $LIMIT_DIALECT = $self->{limit_dialect};

    my $prefix = delete $opts->{prefix} || 'SELECT';
    my $format = "$prefix %c FROM %t";
    if (keys %{ $where || {} }) {
        $format .= ' WHERE %w';
    }
    if (keys %$opts) {
        $format .= ' %o';
    }

    sqlf($format, {
        options => $opts,
        table   => $table,
        columns => $cols,
        where   => $where,
    });
}

sub insert {
    my ($self, $table, $values, $opts) = @_;
    croak 'Usage: $sqlf->insert($table \%values|\@values, [, \%opts])' unless defined $table && ref $values;

    local $SEPARATOR     = $self->{separator};
    local $NAME_SEP      = $self->{name_sep};
    local $QUOTE_CHAR    = $self->{quote_char};
    local $LIMIT_DIALECT = $self->{limit_dialect};

    my $prefix       = $opts->{prefix} || 'INSERT INTO';
    my $quoted_table = _quote($table);

    my @values = ref $values eq 'HASH' ? %$values : @$values;
    my (@columns, @bind_cols, @bind_params);
    for (my $i = 0; $i < @values; $i += 2) {
        my ($col, $val) = ($values[$i], $values[$i+1]);
        push @columns, _quote($col);
        if (ref $val eq 'SCALAR') {
            # foo => { bar => \'NOW()' }
            push @bind_cols, $$val;
        }
        elsif (ref $val eq 'REF' && ref $$val eq 'ARRAY') {
            # foo => { bar => \['UNIX_TIMSTAMP(?)', '2011-11-11 11:11:11'] }
            my ($stmt, @sub_bind) = @{$$val};
            push @bind_cols, $stmt;
            push @bind_params, @sub_bind;
        }
        else {
            # foo => { bar => 'baz' }
            push @bind_cols, '?';
            push @bind_params, $val;
        }
    }

    my $stmt = "$prefix $quoted_table "
             . '('.join(', ', @columns).') '
             . 'VALUES ('.join($self->{separator}, @bind_cols).')';

    return ($stmt, @bind_params);
}

sub update {
    my ($self, $table, $args, $where, $opts) = @_;
    my $prefix = $opts->{prefix} || 'UPDATE';
    my $quote_char = $self->{quote_char};
    my $quoted_tale = "$quote_char$table$quote_char";

    my @args = ref $args eq 'HASH' ? %$args : @$args;
    my (@columns, @bind_params);
    for (my ($i, $l) = (0, scalar @args); $i < $l; $i += 2) {
        my ($col, $val) = ($args[$i], $args[$i+1]);
        my $quoted_col = "$quote_char$col$quote_char";
        if (ref $val eq 'SCALAR') {
            # foo => { bar => \'NOW()' }
            push @columns, "$quoted_col = $$val";
        }
        elsif (ref $val eq 'REF' && ref $$val eq 'ARRAY') {
            # foo => { bar => \['UNIX_TIMSTAMP(?)', '2011-11-11 11:11:11'] }
            my ($stmt, @sub_bind) = @{$$val};
            push @columns, "$quoted_col = $stmt";
            push @bind_params, @sub_bind;
        }
        else {
            # foo => { bar => 'baz' }
            push @columns, "$quoted_col = ?";
            push @bind_params, $val;
        }
    }

    my $format = "$prefix $quoted_tale SET ".join($self->{separator}, @columns);
    if ($where) {
        $format .= ' WHERE %w';
    }
    if ($opts->{order_by}) {
        # TODO
    }
    if ($opts->{limit}) {
        # TODO
    }

    local $SEPARATOR  = $self->{separator};
    local $NAME_SEP   = $self->{name_sep};
    local $QUOTE_CHAR = $self->{quote_char};
    my ($stmt, @bind) = sqlf($format, {
        %$opts,
        where => $where,
    });

    return $stmt, (@bind_params, @bind);
}

sub delete {
    my ($self, $table, $where, $opts) = @_;
    croak 'Usage: $sqlf->delete($table [, \%where, \%opts])' unless defined $table;

    local $SEPARATOR     = $self->{separator};
    local $NAME_SEP      = $self->{name_sep};
    local $QUOTE_CHAR    = $self->{quote_char};
    local $LIMIT_DIALECT = $self->{limit_dialect};

    my $prefix       = delete $opts->{prefix} || 'DELETE';
    my $quoted_table = _quote($table);
    my $format       = "$prefix FROM $quoted_table";

    if (keys %{ $where || {} }) {
        $format .= ' WHERE %w';
    }
    if (keys %$opts) {
        $format .= ' %o';
    }

    sqlf($format, {
        options => $opts,
        where   => $where,
    });
}

1;
__END__

=encoding utf-8

=for stopwords

=head1 NAME

SQL::Format -

=head1 SYNOPSIS

  use SQL::Format;

=head1 DESCRIPTION

SQL::Format is

=head1 AUTHOR

xaicron E<lt>xaicron {at} cpan.orgE<gt>

=head1 COPYRIGHT

Copyright 2011 - xaicron

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=head1 SEE ALSO

=cut
