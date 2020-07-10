"""
Small utility for creating shareable links to Civis services.
"""
import argparse

import civis


def list_services(args):
    client = civis.APIClient()
    services = client.services.list()
    print("List of services:")
    for service in services:
        print(f"\tID: {service['id']}\tName: {service['name']}")


def share_service(args):
    client = civis.APIClient()
    service = client.services.get(args.id)
    try:
        response = client.services.post_tokens(args.id, args.name)
        url = f"{service['current_url']}/civis-platform-auth?token={response['token']}"
        print(f"Share service id {args.id} with the following URL: {url}")
    except civis.base.CivisAPIError as e:
        if "Name has already been taken" in str(e):
            print(
                f"The share name {args.name} is already in use. "
                "Please choose another"
            )
        else:
            raise e


def unshare_service(args):
    client = civis.APIClient()
    tokens = client.services.list_tokens(args.id)
    try:
        token = next(t for t in tokens if t["name"] == args.name)
        client.services.delete_tokens(args.id, token["id"])
        print(f"Successfully unshared {args.name}")
    except StopIteration:
        print(f"Could not find share token with the name {args.name}")


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    list_parser = subparsers.add_parser("list")
    list_parser.set_defaults(func=list_services)

    share_parser = subparsers.add_parser("share")
    share_parser.add_argument("id", type=int, help="The ID of the service to share")
    share_parser.add_argument("name", type=str, help="A name to give the share URL")
    share_parser.set_defaults(func=share_service)

    unshare_parser = subparsers.add_parser("unshare")
    unshare_parser.add_argument("id", type=int, help="The ID of the service to unshare")
    unshare_parser.add_argument(
        "name", type=str, help="The name of the service to unshare"
    )
    unshare_parser.set_defaults(func=unshare_service)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
