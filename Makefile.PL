use inc::Module::Install;
use Module::Install::AuthorTests;

name 'SQL-Format';
all_from 'lib/SQL/Format.pm';

#requires '';

test_requires 'Test::More', 0.98;

tests join q{ }, map { sprintf 't%s.t', '/*' x $_ } 1..3;
author_tests 'xt';

repository 'http://github.com/xaicron/p5-SQL-Format/';
bugtracker 'http://github.com/xaicron/p5-SQL-Format/issues';

WriteAll;

package MY;

# do not install README.pod
sub init_dirscan {
    my $self = shift;
    $self->SUPER::init_dirscan();
    delete $self->{PM}{'README.pod'};
}
