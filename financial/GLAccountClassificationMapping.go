package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/storage"

	errortools "github.com/leapforce-libraries/go_errortools"
	financial "github.com/leapforce-libraries/go_exactonline_new/financial"
	types "github.com/leapforce-libraries/go_types"
)

type GLAccountClassificationMapping struct {
	SoftwareClientLicenseGuid_ string
	Created_                   time.Time
	Modified_                  time.Time
	ID                         string
	Classification             string
	ClassificationCode         string
	ClassificationDescription  string
	Division                   int64
	GLAccount                  string
	GLAccountCode              string
	GLAccountDescription       string
	GLSchemeCode               string
	GLSchemeDescription        string
	GLSchemeID                 string
}

func getGLAccountClassificationMapping(c *financial.GLAccountClassificationMapping, softwareClientLicenseGuid string) GLAccountClassificationMapping {
	t := time.Now()

	return GLAccountClassificationMapping{
		softwareClientLicenseGuid,
		t, t,
		c.ID.String(),
		c.Classification.String(),
		c.ClassificationCode,
		c.ClassificationDescription,
		c.Division,
		c.GLAccount.String(),
		c.GLAccountCode,
		c.GLAccountDescription,
		c.GLSchemeCode,
		c.GLSchemeDescription,
		c.GLSchemeID.String(),
	}
}

func (service *Service) WriteGLAccountClassificationMappings(bucketHandle *storage.BucketHandle, softwareClientLicenseGuid string, _ *time.Time) ([]*storage.ObjectHandle, int, interface{}, *errortools.Error) {
	if bucketHandle == nil {
		return nil, 0, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.FinancialService().NewGetGLAccountClassificationMappingsCall()

	rowCount := 0
	batchRowCount := 0
	batchSize := 10000

	for {
		glAccountClassificationMappings, e := call.Do()
		if e != nil {
			return nil, 0, nil, e
		}

		if glAccountClassificationMappings == nil {
			break
		}

		if batchRowCount == 0 {
			guid := types.NewGuid()
			objectHandle := bucketHandle.Object((&guid).String())
			objectHandles = append(objectHandles, objectHandle)

			w = objectHandle.NewWriter(context.Background())
		}

		for _, tl := range *glAccountClassificationMappings {
			batchRowCount++

			b, err := json.Marshal(getGLAccountClassificationMapping(&tl, softwareClientLicenseGuid))
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}

			// Write data
			_, err = w.Write(b)
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}

			// Write NewLine
			_, err = fmt.Fprintf(w, "\n")
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}
		}

		if batchRowCount > batchSize {
			// Close and flush data
			err := w.Close()
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}
			w = nil

			fmt.Printf("#GLAccountClassificationMappings flushed: %v\n", batchRowCount)

			rowCount += batchRowCount
			batchRowCount = 0
		}
	}

	if w != nil {
		// Close and flush data
		err := w.Close()
		if err != nil {
			return nil, 0, nil, errortools.ErrorMessage(err)
		}

		rowCount += batchRowCount
	}

	fmt.Printf("#GLAccountClassificationMappings: %v\n", rowCount)

	return objectHandles, rowCount, GLAccountClassificationMapping{}, nil
}
