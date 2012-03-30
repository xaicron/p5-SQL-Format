package SQL::Format;

use strict;
use warnings;
use 5.008_001;
our $VERSION = '0.01';

use Exporter 'import';
use Carp qw(croak carp);

our @EXPORT = qw(sqlf);

our $SEPARATOR  = ', ';
our $NAME_SEP   = '.';
our $QUOTE_CHAR = '`';

my $FMAP = {
    '%c' => 'columns',
    '%t' => 'table',
    '%w' => 'where',
    '%o' => 'order_by',
    '%L' => 'limit',
    '%O' => 'offset',
};

my $OP_ALIAS = {
    -IN          => 'IN',
    -NOT_IN      => 'NOT IN',
    -BETWEEN     => 'BETWEEN',
    -NOT_BETWEEN => 'NOT BETWEEN',
    -LIKE        => 'LIKE',
    -NOT_LIKE    => 'NOT LIKE',
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
    }

    my $ret;
    $format =~ s#(\%[ctwoLO])#
        $ret = $args->{$FMAP->{$1}};
        croak "'$1' must be specified '$FMAP->{$1}' field" unless defined $ret;
        $ret;
    #gem;

    return $format, @bind;
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
    $args{separator}  = $SEPARATOR  unless defined $args{separator};
    $args{name_sep}   = $NAME_SEP   unless defined $args{name_sep};
    $args{quote_char} = $QUOTE_CHAR unless defined $args{quote_char};
    bless { %args }, $class;
}

sub select {
    my ($self, $table, $col, $where, $opts) = @_;
    $opts ||= {};
    my $format = "SELECT %c FROM $self->{quote_char}$table$self->{quote_char}";
    if ($where) {
        $format .= ' WHERE %w';
    }
    if ($opts->{order_by}) {
        # TODO
    }
    if ($opts->{limit}) {
        # TODO
    }
    if ($opts->{offset}) {
        # TODO
    }
    local $SEPARATOR  = $self->{separator};
    local $NAME_SEP   = $self->{name_sep};
    local $QUOTE_CHAR = $self->{quote_char};
    sqlf($format, {
        %$opts,
        columns => $col,
        where   => $where,
    });
}

sub insert {
    my ($self, $table, $values, $opts) = @_;
    my $prefix = $opts->{prefix} || 'INSERT INTO';
    my $quote_char = $self->{quote_char};
    my $quoted_tale = "$quote_char$table$quote_char";

    my @values = ref $values eq 'HASH' ? %$values : @$values;
    my (@columns, @bind_cols, @bind_params);
    for (my ($i, $l) = (0, scalar @values); $i < $l; $i += 2) {
        my ($col, $val) = ($values[$i], $values[$i+1]);
        push @columns, "$quote_char$col$quote_char";
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

    my $stmt = "$prefix $quoted_tale "
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
    my $prefix = $opts->{prefix} || 'DELETE FROM';
    my $quote_char = $self->{quote_char};
    my $quoted_tale = "$quote_char$table$quote_char";

    my $format = "$prefix $quoted_tale";
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
    sqlf($format, {
        %$opts,
        where => $where,
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
