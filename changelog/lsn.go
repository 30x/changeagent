/*
 * Functions related to manipulating LSNs.
 * An LSN is an unsigned 64-bit quantity that identifies an extent and
 * a position within that extent.
 * Each is a 32-bit quantity, so we support 2^32 extents and a maximum
 * file size of 2^32, so that should be enough.
 */

package changelog
