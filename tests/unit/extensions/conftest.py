"""Skip extension unit tests when running against compiled modules."""

from tests.conftest import requires_interpreted

pytestmark = requires_interpreted
