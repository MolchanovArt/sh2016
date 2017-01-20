/**
  * Created by mart on 17.01.17.
  */
object FriendshipHelpers {
  // main masks
  val M_LOVE: Int = 1 << 1
  val M_SPOUSE: Int = 1 << 2
  val M_PARENT: Int = 1 << 3
  val M_CHILD: Int = 1 << 4
  val M_BRO_SIS: Int = 1 << 5
  val M_UNCLE_AUNT: Int = 1 << 6
  val M_RELATIVE: Int = 1 << 7
  val M_BEST_FRIEND: Int = 1 << 8
  val M_COLLEAGUE: Int = 1 << 9
  val M_SCHOOLMATE: Int = 1 << 10
  val M_NEPHEW: Int = 1 << 11
  val M_GRANDPARENT: Int = 1 << 12
  val M_GRANDCHILD: Int = 1 << 13
  val M_UNIV_FEL: Int = 1 << 14
  val M_ARMY_FEL: Int = 1 << 15
  val M_PARENT_IN_LAW: Int = 1 << 16
  val M_CHILD_IN_LAW: Int = 1 << 17
  val M_GODPARENT: Int = 1 << 18
  val M_GODCHILD: Int = 1 << 19
  val M_PLAY_TOGETHER: Int = 1 << 20

  // custom general masks
  val FAMILY: Int = 1
  val BEST_FAMILY: Int = 1 << 1
  val BEST_FRIENDS: Int = 1 << 2
  val SCHOOL: Int = 1 << 3
  val WORK: Int = 1 << 4
  val UNIVERSITY: Int = 1 << 5
  val ARMY: Int = 1 << 6
  val PLAY: Int = 1 << 7

  val FAMILY_CNT_SHIFT = 0
  val BEST_FAMILY_CNT_SHIFT = 3
  val BEST_FRIENDS_CNT_SHIFT = 6
  val SCHOOL_CNT_SHIFT = 9
  val WORK_CNT_SHIFT = 12
  val UNIVERSITY_CNT_SHIFT = 15
  val ARMY_CNT_SHIFT = 18
  val PLAY_CNT_SHIFT = 21

  val FAMILY_CNT_MASK: Int = 111 << FAMILY_CNT_SHIFT
  val BEST_FAMILY_CNT_MASK: Int = 111 << BEST_FAMILY_CNT_SHIFT
  val BEST_FRIENDS_CNT_MASK: Int = 111 << BEST_FRIENDS_CNT_SHIFT
  val SCHOOL_CNT_MASK: Int = 111 << SCHOOL_CNT_SHIFT
  val WORK_CNT_MASK: Int = 111 << WORK_CNT_SHIFT
  val UNIVERSITY_CNT_MASK: Int = 111 << UNIVERSITY_CNT_SHIFT
  val ARMY_CNT_MASK: Int = 111 << ARMY_CNT_SHIFT
  val PLAY_CNT_MASK: Int = 111 << PLAY_CNT_SHIFT

  def normalizeFriendshipType(fType: Int): Int = {
    fType - fType % 2
  }

  def invertFriendshipType(fType: Int): Int = {
    var result = fType
    if ((result & M_PARENT) > 0) {
      result -= M_PARENT
      result |= M_CHILD
    }

    if ((result & M_CHILD) > 0) {
      result -= M_CHILD
      result |= M_PARENT
    }

    if ((result & M_UNCLE_AUNT) > 0) {
      result -= M_UNCLE_AUNT
      result |= M_NEPHEW
    }

    if ((result & M_NEPHEW) > 0) {
      result -= M_NEPHEW
      result |= M_UNCLE_AUNT
    }

    if ((result & M_GRANDPARENT) > 0) {
      result -= M_GRANDPARENT
      result |= M_GRANDCHILD
    }

    if ((result & M_PARENT_IN_LAW) > 0) {
      result -= M_PARENT_IN_LAW
      result |= M_CHILD_IN_LAW
    }

    if ((result & M_CHILD_IN_LAW) > 0) {
      result -= M_CHILD_IN_LAW
      result |= M_PARENT_IN_LAW
    }

    if ((result & M_GODPARENT) > 0) {
      result -= M_GODPARENT
      result |= M_GODCHILD
    }

    if ((result & M_GODCHILD) > 0) {
      result -= M_GODCHILD
      result |= M_GODPARENT
    }

    result
  }

  protected def transformFriendshipType(fType: Int): Int = {
    var result = 0

    if (fType == 0) {
      return result
    }

    if ((fType & M_LOVE) > 0) {
      result |= BEST_FRIENDS
      result |= SCHOOL
    }

    if ((fType & M_SPOUSE) > 0) {
      result |= BEST_FAMILY
      result |= FAMILY
      result |= BEST_FRIENDS
    }

    if ((fType & M_PARENT) > 0) {
      result |= BEST_FAMILY
      result |= FAMILY
      result |= BEST_FRIENDS
    }

    if ((fType & M_CHILD) > 0) {
      result |= BEST_FAMILY
      result |= FAMILY
    }

    if ((fType & M_BRO_SIS) > 0) {
      result |= BEST_FAMILY
      result |= FAMILY
    }

    if ((fType & M_UNCLE_AUNT) > 0) {
      result |= FAMILY
    }

    if ((fType & M_RELATIVE) > 0) {
      result |= FAMILY
    }

    if ((fType & M_BEST_FRIEND) > 0) {
      result |= BEST_FAMILY
      result |= BEST_FRIENDS
    }

    if ((fType & M_COLLEAGUE) > 0) {
      result |= WORK
    }
    if ((fType & M_SCHOOLMATE) > 0) {
      result |= SCHOOL
    }
    if ((fType & M_NEPHEW) > 0) {
      result |= FAMILY
    }
    if ((fType & M_GRANDPARENT) > 0) {
      result |= FAMILY
    }
    if ((fType & M_GRANDCHILD) > 0) {
      result |= FAMILY
    }
    if ((fType & M_UNIV_FEL) > 0) {
      result |= UNIVERSITY
    }
    if ((fType & M_ARMY_FEL) > 0) {
      result |= ARMY
    }
    if ((fType & M_PARENT_IN_LAW) > 0) {
      result |= FAMILY
    }
    if ((fType & M_CHILD_IN_LAW) > 0) {
      result |= FAMILY
    }
    if ((fType & M_GODPARENT) > 0) {
      result |= FAMILY
    }
    if ((fType & M_GODCHILD) > 0) {
      result |= FAMILY
    }
    if ((fType & M_PLAY_TOGETHER) > 0) {
      result |= PLAY
    }

    result
  }

  protected def mainTransformFType(fType: Int): Int = {
    var result = 0

    if (fType == 0) {
      return result
    }
    if ((fType & M_LOVE) > 0) {
      result |= BEST_FRIENDS
    }
    if ((fType & M_SPOUSE) > 0) {
      result |= BEST_FAMILY
    }
    if ((fType & M_PARENT) > 0) {
      result |= BEST_FAMILY
    }
    if ((fType & M_CHILD) > 0) {
      result |= BEST_FAMILY
    }
    if ((fType & M_BRO_SIS) > 0) {
      result |= BEST_FAMILY
    }
    if ((fType & M_UNCLE_AUNT) > 0) {
      result |= FAMILY
    }
    if ((fType & M_RELATIVE) > 0) {
      result |= FAMILY
    }
    if ((fType & M_BEST_FRIEND) > 0) {
      result |= BEST_FRIENDS
    }
    if ((fType & M_COLLEAGUE) > 0) {
      result |= WORK
    }
    if ((fType & M_SCHOOLMATE) > 0) {
      result |= SCHOOL
    }
    if ((fType & M_NEPHEW) > 0) {
      result |= FAMILY
    }
    if ((fType & M_GRANDPARENT) > 0) {
      result |= FAMILY
    }
    if ((fType & M_GRANDCHILD) > 0) {
      result |= FAMILY
    }
    if ((fType & M_UNIV_FEL) > 0) {
      result |= UNIVERSITY
    }
    if ((fType & M_ARMY_FEL) > 0) {
      result |= ARMY
    }
    if ((fType & M_PARENT_IN_LAW) > 0) {
      result |= FAMILY
    }
    if ((fType & M_CHILD_IN_LAW) > 0) {
      result |= FAMILY
    }
    if ((fType & M_GODPARENT) > 0) {
      result |= FAMILY
    }
    if ((fType & M_GODCHILD) > 0) {
      result |= FAMILY
    }
    if ((fType & M_PLAY_TOGETHER) > 0) {
      result |= PLAY
    }

    result
  }

  protected def getMainFriendshipFromMask(transformedFType: Int): Int = {
    if ((transformedFType & BEST_FAMILY) > 0) {
      return BEST_FAMILY
    }
    if ((transformedFType & BEST_FRIENDS) > 0) {
      return BEST_FRIENDS
    }
    if ((transformedFType & FAMILY) > 0) {
      return FAMILY
    }
    if ((transformedFType & SCHOOL) > 0) {
      return SCHOOL
    }
    if ((transformedFType & WORK) > 0) {
      return WORK
    }
    if ((transformedFType & UNIVERSITY) > 0) {
      return UNIVERSITY
    }
    if ((transformedFType & ARMY) > 0) {
      return ARMY
    }
    if ((transformedFType & PLAY) > 0) {
      return PLAY
    }
    0
  }

  def combineFTypesToTransFType(fType1: Int, fType2: Int): Int = {
    if (fType1 == 0 || fType2 == 0) {
      0
    } else if (fType1 == fType2) {
      getMainFriendshipFromMask(mainTransformFType(fType1))
    } else {
      getMainFriendshipFromMask(transformFriendshipType(fType1) &
        transformFriendshipType(fType2))
    }
  }

  def transformedFriendTypeToFAcc(transformedFriendType: Int): Int = {
    if (transformedFriendType == 0) {
      0
    }
    var result = 0
    result = accumulateTransformedFriendType(
      result,
      transformedFriendType,
      FAMILY, FAMILY_CNT_MASK, FAMILY_CNT_SHIFT)
    result = accumulateTransformedFriendType(
      result,
      transformedFriendType,
      BEST_FRIENDS, BEST_FRIENDS_CNT_MASK, BEST_FRIENDS_CNT_SHIFT)
    result = accumulateTransformedFriendType(
      result,
      transformedFriendType,
      SCHOOL, SCHOOL_CNT_MASK, SCHOOL_CNT_SHIFT)
    result = accumulateTransformedFriendType(
      result,
      transformedFriendType,
      WORK, WORK_CNT_MASK, WORK_CNT_SHIFT)
    result = accumulateTransformedFriendType(
      result,
      transformedFriendType,
      UNIVERSITY, UNIVERSITY_CNT_MASK, UNIVERSITY_CNT_SHIFT)
    result = accumulateTransformedFriendType(
      result,
      transformedFriendType,
      ARMY, ARMY_CNT_MASK, ARMY_CNT_SHIFT)
    result = accumulateTransformedFriendType(
      result,
      transformedFriendType,
      PLAY, PLAY_CNT_MASK, PLAY_CNT_SHIFT)
    result
  }

  def mergeAccs(acc1: Int, acc2: Int): Int = {
    if (acc1 == 0) {
      acc2
    }
    if (acc2 == 0) {
      acc1
    }

    var result = acc1
    result = mergeAccs(result, acc2, FAMILY_CNT_MASK, FAMILY_CNT_SHIFT)
    result = mergeAccs(result, acc2, BEST_FRIENDS_CNT_MASK, BEST_FRIENDS_CNT_SHIFT)
    result = mergeAccs(result, acc2, SCHOOL_CNT_MASK, SCHOOL_CNT_SHIFT)
    result = mergeAccs(result, acc2, WORK_CNT_MASK, WORK_CNT_SHIFT)
    result = mergeAccs(result, acc2, UNIVERSITY_CNT_MASK, UNIVERSITY_CNT_SHIFT)
    result = mergeAccs(result, acc2, ARMY_CNT_MASK, ARMY_CNT_SHIFT)
    result = mergeAccs(result, acc2, PLAY_CNT_MASK, PLAY_CNT_SHIFT)
    result
  }

  private def accumulateTransformedFriendType(acc: Int,
                                              transformedFriendType: Int,
                                              transformationType: Int,
                                              transformationMask: Int,
                                              transformationOffset: Int): Int = {
    if ((transformedFriendType & transformationType) > 0) {
      val currentCount = (acc & transformationMask) >> transformationOffset
      val count = currentCount + 1
      if (count < 8) {
        (acc & ~transformationMask) + (count << transformationOffset)
      } else {
        acc
      }
    } else {
      acc
    }
  }

  private def mergeAccs(acc1: Int, acc2: Int,
                        transformationMask: Int,
                        transformationOffset: Int): Int = {
    val count = getFriendTypeCountAcc(acc1, transformationMask, transformationOffset) +
      getFriendTypeCountAcc(acc2, transformationMask, transformationOffset)
    if (count < 8) {
      (acc1 & ~transformationMask) + (count << transformationOffset)
    }
    else {
      (acc1 & ~transformationMask) + (7 << transformationOffset)
    }
  }

  def getFriendTypeCountAcc(acc: Int,
                            transformationMask: Int,
                            transformationOffset: Int): Int = {
    (acc & transformationMask) >> transformationOffset
  }
}


