#!/bin/bash
# Copyright (c) 2026 VillageSQL Contributors
#
# Add a non-root user to docker image.
# Env vars:
# USER_ID    - uid for the user
# USER_NAME  - username for the user
# GROUP_ID   - gid for the group the user would be part of
# GROUP_NAME - group name for the group

# Ubuntu images come prebuilt with user(ubuntu, 1000) and group(ubuntu, 1000). # Delete existing user(if any) on USER_ID.
if getent passwd "${USER_ID}" >/dev/null; then
  userdel "$(getent passwd "${USER_ID}" | cut -d: -f1)"
fi

# Delete existing group(if any) on GROUP_ID
if getent group "${GROUP_ID}" >/dev/null; then
  groupdel "$(getent group "${GROUP_ID}" | cut -d: -f1)"
fi

# USER_NAME might already exist on a different uid other than USER_ID
# Delete it if so.
if getent passwd "${USER_NAME}" >/dev/null; then
  userdel "${USER_NAME}"
fi

# GROUP_NAME might already exist on a different gid other than GROUP_ID
# Delete it if so.
if getent group "${GROUP_NAME}" >/dev/null; then
  groupdel "${GROUP_NAME}"
fi

# Add (GROUP_ID, GROUP_NAME)
groupadd -g "${GROUP_ID}" "${GROUP_NAME}"

# Add (USER_ID, USER_NAME) within GROUP_ID
useradd -l -u "${USER_ID}" -g "${GROUP_ID}" -m -s /bin/bash "${USER_NAME}"

# Grant sudo privileges to USER_NAME
usermod -a -G sudo "${USER_NAME}"

# Enable all sudoers to use sudo without password
echo '%sudo ALL=(ALL) NOPASSWD:ALL' >>/etc/sudoers
