"""Bulk users for load/chaos tests: load_1..load_N with ids first_id..first_id+N-1.

    docker compose exec web python manage.py seed_load_users --count 500000

Passwords are unusable ("!") so no hashing: 500k users take seconds, not hours.
Load scripts sign their own JWTs for user ids in the printed range.
"""
from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand, CommandError


class Command(BaseCommand):
    help = "Ensure users <prefix>1..<prefix>N exist and print their (contiguous) id range."

    def add_arguments(self, parser):
        parser.add_argument("--count", type=int, default=500_000)
        parser.add_argument("--prefix", default="load_")
        parser.add_argument("--batch", type=int, default=5_000)

    def handle(self, *args, **opts):
        User = get_user_model()
        prefix, count = opts["prefix"], opts["count"]

        for start in range(1, count + 1, opts["batch"]):
            stop = min(start + opts["batch"], count + 1)
            User.objects.bulk_create(
                [User(username=f"{prefix}{i}", password="!") for i in range(start, stop)],
                ignore_conflicts=True,  # re-running only adds the missing ones
            )
            if (stop - 1) % 50_000 == 0:
                self.stdout.write(f"  ... {stop - 1} users")

        first = User.objects.get(username=f"{prefix}1").id
        last = User.objects.get(username=f"{prefix}{count}").id
        # user <prefix>i must have id first + i - 1, otherwise signed ids would miss real users.
        gaps = 0
        rows = User.objects.filter(username__startswith=prefix).values_list("id", "username")
        for user_id, username in rows.iterator(chunk_size=10_000):
            suffix = username[len(prefix):]
            if suffix.isdigit() and int(suffix) <= count and user_id != first + int(suffix) - 1:
                gaps += 1
        if gaps:
            # Happens when an existing block is extended after other users were created.
            raise CommandError(f"ids are not contiguous ({gaps} mismatches); use a new --prefix for this block size")
        self.stdout.write(self.style.SUCCESS(f"load users: {prefix}1..{prefix}{count} first_id={first} last_id={last}"))
